package persistence

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/lisama569/runtime-config-control-plane-go/internal/errs"
)

// Default audit journal capacity bounds.
const (
	// MaxAuditEntries bounds how many audit entries survive on disk before
	// the journal is compacted.
	MaxAuditEntries = 10000
	// AuditCompactRetain is how many newest entries survive a compaction.
	AuditCompactRetain = 5000
)

// Frame layout (all integers big-endian):
//
//	[0:4]   magic "RCA1"
//	[4:8]   uint32 payload length N
//	[8:8+N] JSON payload
//	[8+N:40+N] SHA-256(magic || length || payload)
const (
	auditMagic    = "RCA1"
	auditHdrLen   = 8
	auditSumLen   = sha256.Size
	auditMaxFrame = 4 << 20 // 4 MiB per audit entry, to reject garbage
)

// AuditSink is a checksummed, append-only framed journal for audit entries.
// Each append is one write followed by an fsync, so committed audit records
// survive a kill/power loss. A torn final frame (partial write) is truncated
// on recovery; non-tail corruption is a classified error.
type AuditSink struct {
	path    string
	logger  *log.Logger
	maxKeep int
	count   int
	loaded  bool
}

// NewAuditSink opens the audit journal at path.
func NewAuditSink(path string, logger *log.Logger) *AuditSink {
	if logger == nil {
		logger = log.Default()
	}
	return &AuditSink{path: path, logger: logger, maxKeep: MaxAuditEntries}
}

// Path returns the journal file location.
func (a *AuditSink) Path() string { return a.path }

// Append durably appends one already-JSON-encoded entry (single write +
// fsync). A torn tail left by a previous crash is repaired first.
func (a *AuditSink) Append(payload []byte) error {
	if !a.loaded {
		return errs.New(errs.KindPersistFailed,
			"persistence: audit sink used before Load")
	}
	if len(payload) == 0 {
		return errs.New(errs.KindValidation, "persistence: empty audit payload")
	}
	frame, err := buildFrame(payload)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(a.path), 0o700); err != nil {
		return errs.Wrap(errs.KindPersistFailed,
			"persistence: cannot create audit directory", err)
	}
	f, err := os.OpenFile(a.path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return errs.Wrap(errs.KindPersistFailed,
			"persistence: cannot open audit journal", err)
	}
	if _, err := f.Write(frame); err != nil {
		_ = f.Close()
		return errs.Wrap(errs.KindPersistFailed,
			"persistence: cannot append audit frame", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return errs.Wrap(errs.KindPersistFailed,
			"persistence: cannot fsync audit journal", err)
	}
	if err := f.Close(); err != nil {
		return errs.Wrap(errs.KindPersistFailed,
			"persistence: cannot close audit journal", err)
	}
	a.count++
	if a.count > a.maxKeep {
		if err := a.Compact(AuditCompactRetain); err != nil {
			return err
		}
	}
	return nil
}

// Load reads all intact frames. A torn tail is physically truncated (it can
// only ever be the in-flight final append of a crashed process); corruption
// earlier in the journal is STATE_CORRUPT and nothing is returned.
func (a *AuditSink) Load() (payloads [][]byte, truncated bool, err error) {
	data, rerr := os.ReadFile(a.path)
	if rerr != nil {
		if os.IsNotExist(rerr) {
			a.count = 0
			a.loaded = true
			return nil, false, nil
		}
		return nil, false, errs.Wrap(errs.KindPersistFailed,
			"persistence: cannot read audit journal", rerr)
	}
	pos := 0
	for pos < len(data) {
		plen, frameLen, torn, perr := frameAt(data, pos)
		if perr != nil {
			// Only a partial final frame is repairable: the torn bytes reach
			// exactly EOF. Anything else means damage inside the journal.
			if torn {
				if terr := os.Truncate(a.path, int64(pos)); terr != nil {
					return nil, false, errs.Wrap(errs.KindPersistFailed,
						"persistence: cannot truncate torn audit tail", terr)
				}
				a.logger.Printf("persistence: truncated %d torn byte(s) from audit journal tail",
					len(data)-pos)
				a.count = len(payloads)
				a.loaded = true
				return payloads, true, nil
			}
			return nil, false, perr
		}
		payloads = append(payloads, append([]byte(nil), data[pos+auditHdrLen:pos+auditHdrLen+plen]...))
		pos += frameLen
	}
	a.count = len(payloads)
	a.loaded = true
	return payloads, false, nil
}

// Compact rewrites the journal keeping at most retain newest frames.
func (a *AuditSink) Compact(retain int) error {
	data, err := os.ReadFile(a.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return errs.Wrap(errs.KindPersistFailed,
			"persistence: cannot read audit journal for compaction", err)
	}
	var frames [][]byte
	pos := 0
	for pos < len(data) {
		_, frameLen, torn, perr := frameAt(data, pos)
		if perr != nil {
			if torn {
				break
			}
			return perr
		}
		frames = append(frames, append([]byte(nil), data[pos:pos+frameLen]...))
		pos += frameLen
	}
	if retain >= 0 && len(frames) > retain {
		frames = frames[len(frames)-retain:]
	}
	tmp := a.path + ".compact.tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return errs.Wrap(errs.KindPersistFailed,
			"persistence: cannot open compacted audit journal", err)
	}
	for _, fr := range frames {
		if _, werr := f.Write(fr); werr != nil {
			_ = f.Close()
			_ = os.Remove(tmp)
			return errs.Wrap(errs.KindPersistFailed,
				"persistence: cannot write compacted audit journal", werr)
		}
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return errs.Wrap(errs.KindPersistFailed,
			"persistence: cannot fsync compacted audit journal", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return errs.Wrap(errs.KindPersistFailed,
			"persistence: cannot close compacted audit journal", err)
	}
	if err := os.Rename(tmp, a.path); err != nil {
		_ = os.Remove(tmp)
		return errs.Wrap(errs.KindPersistFailed,
			"persistence: cannot publish compacted audit journal", err)
	}
	if err := syncDir(filepath.Dir(a.path)); err != nil {
		return errs.Wrap(errs.KindPersistFailed,
			"persistence: cannot fsync audit directory", err)
	}
	a.count = len(frames)
	a.logger.Printf("persistence: audit journal compacted to %d entries", len(frames))
	return nil
}

func buildFrame(payload []byte) ([]byte, error) {
	if len(payload) > auditMaxFrame {
		return nil, errs.New(errs.KindPersistFailed, fmt.Sprintf(
			"persistence: audit payload %d bytes exceeds frame limit", len(payload)))
	}
	frame := make([]byte, auditHdrLen+len(payload)+auditSumLen)
	copy(frame[0:4], auditMagic)
	binary.BigEndian.PutUint32(frame[4:8], uint32(len(payload)))
	copy(frame[auditHdrLen:auditHdrLen+len(payload)], payload)
	sum := sha256.Sum256(frame[:auditHdrLen+len(payload)])
	copy(frame[auditHdrLen+len(payload):], sum[:])
	return frame, nil
}

// frameAt validates the frame starting at off and returns payload length and
// total frame length. torn=true with an error means the frame is incomplete
// precisely at EOF (a crashed partial append), which the caller may repair;
// every other failure is damage inside the journal and must not be ignored.
func frameAt(data []byte, off int) (plen, frameLen int, torn bool, err error) {
	if off+auditHdrLen > len(data) {
		return 0, 0, true, errs.New(errs.KindStateCorrupt,
			"persistence: audit journal header is truncated at EOF")
	}
	if string(data[off:off+4]) != auditMagic {
		return 0, 0, false, errs.New(errs.KindStateCorrupt,
			"persistence: audit journal has bad magic at offset "+itoa64(int64(off)))
	}
	n := binary.BigEndian.Uint32(data[off+4 : off+8])
	if n > auditMaxFrame {
		return 0, 0, false, errs.New(errs.KindStateCorrupt,
			"persistence: audit frame declares implausible length")
	}
	total := auditHdrLen + int(n) + auditSumLen
	if off+total > len(data) {
		return int(n), total, true, errs.New(errs.KindStateCorrupt,
			"persistence: audit journal final frame is truncated at EOF")
	}
	sumEnd := off + auditHdrLen + int(n)
	want := sha256.Sum256(data[off:sumEnd])
	if hex.EncodeToString(data[sumEnd:sumEnd+auditSumLen]) != hex.EncodeToString(want[:]) {
		return 0, 0, false, errs.New(errs.KindStateCorrupt,
			"persistence: audit frame checksum mismatch at offset "+itoa64(int64(off)))
	}
	return int(n), total, false, nil
}

func itoa64(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
