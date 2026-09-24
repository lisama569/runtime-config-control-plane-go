package httpapi

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/lisama569/runtime-config-control-plane-go/internal/config"
	"github.com/lisama569/runtime-config-control-plane-go/internal/controlplane"
	"github.com/lisama569/runtime-config-control-plane-go/internal/security"
)

// Server exposes the control plane over HTTP.
type Server struct {
	plane *controlplane.Plane
	mux   *http.ServeMux
}

// NewServer wires routes.
func NewServer(p *controlplane.Plane) *Server {
	s := &Server{plane: p, mux: http.NewServeMux()}
	s.mux.HandleFunc("/healthz", s.handleHealth)
	s.mux.HandleFunc("/v1/config", s.handleConfig)
	s.mux.HandleFunc("/v1/config/reload", s.handleReload)
	s.mux.HandleFunc("/v1/config/write", s.handleWrite)
	s.mux.HandleFunc("/v1/config/rollback", s.handleRollback)
	s.mux.HandleFunc("/v1/flags/", s.handleEvaluate) // /v1/flags/{name}/evaluate
	s.mux.HandleFunc("/v1/history", s.handleHistory)
	s.mux.HandleFunc("/v1/audit", s.handleAudit)
	return s
}

// Handler returns the root http.Handler.
func (s *Server) Handler() http.Handler { return s.mux }

// PrincipalFromRequest extracts the authenticated principal from
// headers (X-Actor-Id, X-Actor-Name, X-Actor-Roles).
func PrincipalFromRequest(r *http.Request) security.Principal {
	p := security.Principal{
		ID:   r.Header.Get("X-Actor-Id"),
		Name: r.Header.Get("X-Actor-Name"),
	}
	for _, role := range strings.Split(r.Header.Get("X-Actor-Roles"), ",") {
		role = strings.TrimSpace(role)
		if role != "" {
			p.Roles = append(p.Roles, security.Role(role))
		}
	}
	return p
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "version": s.plane.CurrentVersion()})
}

func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, config.NewError(config.CatValidation, "method not allowed"))
		return
	}
	snap, err := s.plane.ReadConfig(PrincipalFromRequest(r))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

type reloadRequest struct {
	ExpectedVersion int64  `json:"expected_version"`
	Reason          string `json:"reason"`
}

func (s *Server) handleReload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, config.NewError(config.CatValidation, "method not allowed"))
		return
	}
	pri := PrincipalFromRequest(r)
	var req reloadRequest
	_ = json.NewDecoder(r.Body).Decode(&req)
	snap, err := s.plane.Reload(r.Context(), pri.Actor(), req.Reason, req.ExpectedVersion)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

type writeRequest struct {
	Config          config.Config `json:"config"`
	ExpectedVersion int64         `json:"expected_version"`
	Reason          string        `json:"reason"`
}

func (s *Server) handleWrite(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, config.NewError(config.CatValidation, "method not allowed"))
		return
	}
	pri := PrincipalFromRequest(r)
	var req writeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, config.NewError(config.CatValidation, "invalid body: %v", err))
		return
	}
	snap, err := s.plane.Write(r.Context(), pri, req.Config, req.ExpectedVersion, req.Reason)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

type rollbackRequest struct {
	TargetVersion   int64  `json:"target_version"`
	ExpectedVersion int64  `json:"expected_version"`
	Reason          string `json:"reason"`
}

func (s *Server) handleRollback(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, config.NewError(config.CatValidation, "method not allowed"))
		return
	}
	pri := PrincipalFromRequest(r)
	var req rollbackRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, config.NewError(config.CatValidation, "invalid body: %v", err))
		return
	}
	snap, err := s.plane.Rollback(r.Context(), pri, req.TargetVersion, req.ExpectedVersion, req.Reason)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

func (s *Server) handleEvaluate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, config.NewError(config.CatValidation, "method not allowed"))
		return
	}
	rest := strings.TrimPrefix(r.URL.Path, "/v1/flags/")
	parts := strings.Split(rest, "/")
	if len(parts) != 2 || parts[1] != "evaluate" || parts[0] == "" {
		writeError(w, config.NewError(config.CatNotFound, "unknown flag route %q", r.URL.Path))
		return
	}
	subject := r.URL.Query().Get("subject")
	if subject == "" {
		writeError(w, config.NewError(config.CatValidation, "subject query parameter is required"))
		return
	}
	dec, snap, err := s.plane.Evaluate(PrincipalFromRequest(r), parts[0], subject)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"flag": dec.Flag, "subject": dec.Subject, "on": dec.On,
		"bucket": dec.Bucket, "threshold": dec.Threshold,
		"reason": dec.Reason, "version": snap.Version,
	})
}

func (s *Server) handleHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, config.NewError(config.CatValidation, "method not allowed"))
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	hist, err := s.plane.History(PrincipalFromRequest(r), limit)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, hist)
}

func (s *Server) handleAudit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, config.NewError(config.CatValidation, "method not allowed"))
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	entries, err := s.plane.AuditLog(PrincipalFromRequest(r), limit)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, entries)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, err error) {
	cat := config.CatOf(err)
	status := http.StatusInternalServerError
	switch cat {
	case config.CatValidation, config.CatTypeMismatch:
		status = http.StatusBadRequest
	case config.CatConflict:
		status = http.StatusConflict
	case config.CatUnauthorized:
		status = http.StatusUnauthorized
	case config.CatForbidden:
		status = http.StatusForbidden
	case config.CatNotFound:
		status = http.StatusNotFound
	}
	writeJSON(w, status, map[string]any{
		"error": map[string]any{"category": cat, "message": err.Error()},
	})
}
