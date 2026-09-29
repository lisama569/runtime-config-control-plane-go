package persist

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/lisama569/runtime-config-control-plane-go/internal/audit"
	"github.com/lisama569/runtime-config-control-plane-go/internal/errs"
)

// AuditSink is the durability hook used by audit.Logger (defined here to
// avoid an import cycle from audit into persist).
type AuditSink interface {
	AppendAudit(ctx context.Context, e audit.Entry) error
}

// Frame layout (all integers big-endian):
//
//	[4 bytes length N][N bytes payload][4 bytes CRC32(payload)]
//
// A frame is only accepted when length + CRC match; a partial trailing frame
// (process killed mid-append) is detected on replay and discarded as a torn
// tail with a classified Problem.
const (
	lenSize  = 4
	crcSize  = 4
	maxFrame = 1 << 20 // a single audit record may not exceed 1 MiB
)

// AppendAudit durably appends one checksummed frame and fsyncs it before
// returning. A failure means the entry is not durable; in-memory state is
// unaffected because audit recording never gates configuration commits.
func (s *Store) AppendAudit(ctx context.Context, e audit.Entry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	payload, err := json.Marshal(&e)
	if err != nil {
		return errs.Wrap(errs.KindPersistIO, "persist: cannot encode audit entry", err)
	}
	frame := make([]byte, 0, lenSize+len(payload)+crcSize)
	frame = binary.BigEndian.AppendUint32(frame, uint32(len(payload)))
	frame = append(frame, payload...)
	frame = binary.BigEndian.AppendUint32(frame, crc32.ChecksumIEEE(payload))
	if len(frame) > maxFrame {
		return errs.New(errs.KindPersistTooLarge,
			fmt.Sprintf("persist: audit frame %d bytes exceeds cap %d", len(frame), maxFrame))
	}

	s.journalMu.Lock()
	defer s.journalMu.Unlock()

	if s.limits.MaxAuditBytes > 0 && int64(len(frame)) > int64(s.limits.MaxAuditBytes) {
		return errs.New(errs.KindPersistTooLarge, "persist: audit frame larger than journal cap")
	}
	// Compact before appending so that, after adding this frame, the journal
	// holds at most MaxAuditEntries (the newest ones).
	needCompact := len(s.auditEntries) >= s.limits.MaxAuditEntries
	if !needCompact && s.limits.MaxAuditBytes > 0 {
		if fi, statErr := os.Stat(auditPath(s.dir)); statErr == nil &&
			fi.Size()+int64(len(frame)) > int64(s.limits.MaxAuditBytes) {
			needCompact = true
		}
	}
	if needCompact {
		// Reserve one slot for the frame being appended.
		if err := s.compactTo(s.limits.MaxAuditEntries - 1); err != nil {
			return err
		}
	}

	f, err := os.OpenFile(auditPath(s.dir), os.O_WRONLY|os.O_CREATE|os.O_APPEND, filePerm)
	if err != nil {
		return errs.Wrap(errs.KindPersistIO, "persist: cannot open audit journal", err)
	}
	if _, err := f.Write(frame); err != nil {
		_ = f.Close()
		return errs.Wrap(errs.KindPersistIO, "persist: cannot append audit frame", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return errs.Wrap(errs.KindPersistIO, "persist: fsync audit frame failed", err)
	}
	if err := f.Close(); err != nil {
		return errs.Wrap(errs.KindPersistIO, "persist: close audit journal failed", err)
	}
	s.auditEntries = append(s.auditEntries, e)
	return nil
}

// LoadAudit replays intact frames in append order. Reading stops at the first
// damaged/truncated frame; the intact prefix is returned and the damage is
// reported (the torn tail is physically truncated on next append).
func (s *Store) LoadAudit(ctx context.Context) ([]audit.Entry, *RecoveryReport, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	f, err := os.Open(auditPath(s.dir))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, &RecoveryReport{}, nil
		}
		return nil, nil, errs.Wrap(errs.KindPersistIO, "persist: cannot open audit journal", err)
	}
	defer f.Close()

	rep := &RecoveryReport{}
	var entries []audit.Entry
	r := io.Reader(f)
	off := int64(0)
	for {
		var lenB [lenSize]byte
		n, rerr := io.ReadFull(r, lenB[:])
		off += int64(n)
		if rerr == io.EOF {
			break // clean end
		}
		if rerr == io.ErrUnexpectedEOF {
			rep.Problems = append(rep.Problems, Problem{
				Kind: string(errs.KindPersistChecksum),
				Message: fmt.Sprintf(
					"persist: audit journal has a torn frame header at byte %d; discarded tail",
					off-int64(n)),
			})
			break
		}
		if rerr != nil {
			return nil, nil, errs.Wrap(errs.KindPersistIO, "persist: cannot read audit journal", rerr)
		}
		pLen := binary.BigEndian.Uint32(lenB[:])
		if pLen == 0 || pLen > maxFrame {
			rep.Problems = append(rep.Problems, Problem{
				Kind: string(errs.KindPersistCorrupt),
				Message: fmt.Sprintf(
					"persist: audit journal frame length %d out of range at byte %d; discarded tail",
					pLen, off-lenSize),
			})
			break
		}
		payload := make([]byte, pLen)
		n, rerr = io.ReadFull(r, payload)
		off += int64(n)
		if rerr != nil {
			rep.Problems = append(rep.Problems, Problem{
				Kind: string(errs.KindPersistChecksum),
				Message: fmt.Sprintf(
					"persist: audit journal has a truncated frame payload at byte %d; discarded tail",
					off-int64(n)),
			})
			break
		}
		var crcB [crcSize]byte
		n, rerr = io.ReadFull(r, crcB[:])
		off += int64(n)
		if rerr != nil {
			rep.Problems = append(rep.Problems, Problem{
				Kind: string(errs.KindPersistChecksum),
				Message: fmt.Sprintf(
					"persist: audit journal has a truncated checksum at byte %d; discarded tail",
					off-int64(n)),
			})
			break
		}
		if binary.BigEndian.Uint32(crcB[:]) != crc32.ChecksumIEEE(payload) {
			rep.Problems = append(rep.Problems, Problem{
				Kind: string(errs.KindPersistChecksum),
				Message: fmt.Sprintf(
					"persist: audit journal frame checksum mismatch at byte %d; discarded tail",
					off-int64(lenSize)-int64(pLen)-crcSize),
			})
			break
		}
		var e audit.Entry
		if jerr := json.Unmarshal(payload, &e); jerr != nil {
			rep.Problems = append(rep.Problems, Problem{
				Kind:    string(errs.KindPersistCorrupt),
				Message: "persist: audit journal frame payload is not valid JSON; discarded tail",
			})
			break
		}
		entries = append(entries, e)
	}

	s.journalMu.Lock()
	s.auditEntries = append(s.auditEntries, entries...)
	s.journalMu.Unlock()
	if len(rep.Problems) > 0 {
		// Physically drop the torn/corrupt tail so damaged bytes never linger.
		if good := validJournalBytes(auditPath(s.dir)); good >= 0 {
			if terr := os.Truncate(auditPath(s.dir), good); terr != nil {
				rep.Problems = append(rep.Problems, Problem{
					Kind:    string(errs.KindPersistIO),
					Message: "persist: could not truncate damaged audit tail: " + terr.Error(),
				})
			}
		}
	}
	return entries, rep, nil
}

// validJournalBytes re-scans the journal and returns the byte length of the
// intact frame prefix (or -1 if the file cannot be opened).
func validJournalBytes(path string) int64 {
	data, err := os.ReadFile(path)
	if err != nil {
		return -1
	}
	var good int64
	for int(good)+lenSize+crcSize <= len(data) {
		pLen := binary.BigEndian.Uint32(data[good : good+lenSize])
		end := good + lenSize + int64(pLen) + crcSize
		if pLen == 0 || pLen > maxFrame || end > int64(len(data)) {
			break
		}
		payload := data[good+lenSize : good+lenSize+int64(pLen)]
		crcAt := good + lenSize + int64(pLen)
		if binary.BigEndian.Uint32(data[crcAt:end]) != crc32.ChecksumIEEE(payload) {
			break
		}
		var e audit.Entry
		if jerr := json.Unmarshal(payload, &e); jerr != nil {
			break
		}
		good = end
	}
	return good
}

// compactTo rewrites the journal keeping at most keepCount newest entries,
// and further shrinks the window to fit MaxAuditBytes when set. It uses the
// same temp-file+rename+fsync protocol as state writes.
func (s *Store) compactTo(keepCount int) error {
	keep := s.auditEntries
	if keepCount >= 0 && len(keep) > keepCount {
		keep = keep[len(keep)-keepCount:]
	}
	frameSize := func(e audit.Entry) int {
		payload, _ := json.Marshal(&e)
		return lenSize + len(payload) + crcSize
	}
	if s.limits.MaxAuditBytes > 0 {
		var size int
		for _, e := range keep {
			size += frameSize(e)
		}
		for len(keep) > 1 && size > s.limits.MaxAuditBytes {
			size -= frameSize(keep[0])
			keep = keep[1:]
		}
	}

	tmp, err := os.CreateTemp(s.dir, ".audit-*"+tmpSuffix)
	if err != nil {
		return errs.Wrap(errs.KindPersistIO, "persist: cannot create audit temp", err)
	}
	tmpName := tmp.Name()
	abort := func() { _ = tmp.Close(); _ = os.Remove(tmpName) }
	for _, e := range keep {
		payload, merr := json.Marshal(&e)
		if merr != nil {
			abort()
			return errs.Wrap(errs.KindPersistIO, "persist: cannot encode audit entry", merr)
		}
		frame := binary.BigEndian.AppendUint32(nil, uint32(len(payload)))
		frame = append(frame, payload...)
		frame = binary.BigEndian.AppendUint32(frame, crc32.ChecksumIEEE(payload))
		if _, werr := tmp.Write(frame); werr != nil {
			abort()
			return errs.Wrap(errs.KindPersistIO, "persist: cannot rewrite audit journal", werr)
		}
	}
	if err := tmp.Sync(); err != nil {
		abort()
		return errs.Wrap(errs.KindPersistIO, "persist: fsync audit temp failed", err)
	}
	if err := tmp.Close(); err != nil {
		abort()
		return errs.Wrap(errs.KindPersistIO, "persist: close audit temp failed", err)
	}
	if err := os.Chmod(tmpName, filePerm); err != nil {
		_ = os.Remove(tmpName)
		return errs.Wrap(errs.KindPersistIO, "persist: chmod audit temp failed", err)
	}
	if err := os.Rename(tmpName, auditPath(s.dir)); err != nil {
		_ = os.Remove(tmpName)
		return errs.Wrap(errs.KindPersistIO, "persist: cannot install compacted journal", err)
	}
	if err := syncDir(s.dir); err != nil {
		return err
	}
	s.auditEntries = append([]audit.Entry(nil), keep...)
	return nil
}

var _ AuditSink = (*Store)(nil)
