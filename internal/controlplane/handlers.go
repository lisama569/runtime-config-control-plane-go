package controlplane

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/lisama569/runtime-config-control-plane-go/internal/auth"
	"github.com/lisama569/runtime-config-control-plane-go/internal/config"
	"github.com/lisama569/runtime-config-control-plane-go/internal/errs"
	"github.com/lisama569/runtime-config-control-plane-go/internal/flag"
)

// PropertyView is a masked, explainable property for API responses.
type PropertyView struct {
	Key     string `json:"key"`
	Type    string `json:"type"`
	Value   string `json:"value"`
	Version int64  `json:"version"`
	Origin  string `json:"origin"`
	Masked  bool   `json:"masked,omitempty"`
}

// FeatureView is an explainable feature definition.
type FeatureView struct {
	Name       string   `json:"name"`
	Enabled    bool     `json:"enabled"`
	Percentage int      `json:"percentage"`
	AllowList  []string `json:"allow_list"`
	Origin     string   `json:"origin"`
}

type configResponse struct {
	Version        int64            `json:"version"`
	Properties     []PropertyView   `json:"properties"`
	Features       []FeatureView    `json:"features"`
	SourceVersions map[string]int64 `json:"source_versions"`
}

type errorResponse struct {
	Error   string `json:"error"`
	Kind    string `json:"kind"`
	Version int64  `json:"version,omitempty"`
}

// Handler exposes the local HTTP API.
func (p *Plane) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/config", p.withAuth(auth.ActRead, p.handleGetConfig))
	mux.HandleFunc("GET /v1/properties/{key}", p.withAuth(auth.ActRead, p.handleGetProp))
	mux.HandleFunc("GET /v1/features/{name}/evaluate", p.withAuth(auth.ActRead, p.handleEvaluate))
	mux.HandleFunc("POST /v1/config", p.withAuth(auth.ActWrite, p.handlePatch))
	mux.HandleFunc("POST /v1/rollback", p.withAuth(auth.ActRollback, p.handleRollback))
	mux.HandleFunc("GET /v1/audit", p.withAuth(auth.ActAuditRead, p.handleAudit))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	return mux
}

type authedHandler func(w http.ResponseWriter, r *http.Request, principal auth.Principal)

// withAuth enforces authentication+authorization. Authentication failures and
// authorization failures are deliberately distinct status codes/error kinds.
func (p *Plane) withAuth(action auth.Action, h authedHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token := bearerToken(r)
		principal, err := p.authz.Check(token, action)
		if err != nil {
			kind := errs.Classify(err)
			status := http.StatusForbidden
			if kind == errs.KindUnauthorized {
				status = http.StatusUnauthorized
			}
			// Record denied attempts; they never changed any state.
			_, _ = p.aud.Record(r.Context(), auditEntryDenied(principal, action, err))
			writeError(w, status, p.Current().Version, err)
			return
		}
		h(w, r, principal)
	}
}

func bearerToken(r *http.Request) string {
	h := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if len(h) > len(prefix) && strings.EqualFold(h[:len(prefix)], prefix) {
		return h[len(prefix):]
	}
	return ""
}

func (p *Plane) handleGetConfig(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	snap := p.Current()
	writeJSON(w, http.StatusOK, buildConfigResponse(snap))
}

func (p *Plane) handleGetProp(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	key := r.PathValue("key")
	snap := p.Current()
	prop, ok := snap.Doc.Properties[key]
	if !ok {
		writeError(w, http.StatusNotFound, snap.Version,
			errs.New(errs.KindNotFound, "property "+key+" not found"))
		return
	}
	writeJSON(w, http.StatusOK, toPropertyView(key, prop, snap.PropertyOrigin[key]))
}

func (p *Plane) handleEvaluate(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	name := r.PathValue("name")
	subject := r.URL.Query().Get("subject")
	snap := p.Current()
	d, err := p.eval.Evaluate(snap, name, subject)
	if err != nil {
		writeError(w, http.StatusBadRequest, snap.Version, err)
		return
	}
	writeJSON(w, http.StatusOK, d)
}

func (p *Plane) handlePatch(w http.ResponseWriter, r *http.Request, principal auth.Principal) {
	var req PatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, p.Current().Version,
			errs.Wrap(errs.KindValidation, "request body is not valid JSON", err))
		return
	}
	from, to, err := p.ApplyPatch(r.Context(), principal.Name, req)
	if err != nil {
		writeKindError(w, p.Current().Version, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int64{"from_version": from, "version": to})
}

type rollbackRequest struct {
	Version int64 `json:"version"`
}

func (p *Plane) handleRollback(w http.ResponseWriter, r *http.Request, principal auth.Principal) {
	var req rollbackRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, p.Current().Version,
			errs.Wrap(errs.KindValidation, "request body is not valid JSON", err))
		return
	}
	from, to, err := p.Rollback(r.Context(), principal.Name, req.Version)
	if err != nil {
		writeKindError(w, p.Current().Version, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int64{
		"restored_from": req.Version,
		"from_version":  from,
		"version":       to,
	})
}

func (p *Plane) handleAudit(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	writeJSON(w, http.StatusOK, map[string]any{"entries": p.aud.List()})
}

func buildConfigResponse(snap *config.Snapshot) configResponse {
	resp := configResponse{
		Version:        snap.Version,
		SourceVersions: snap.SourceVersions,
		Properties:     []PropertyView{},
		Features:       []FeatureView{},
	}
	keys := sortedKeys(snap.Doc.Properties)
	for _, k := range keys {
		resp.Properties = append(resp.Properties,
			toPropertyView(k, snap.Doc.Properties[k], snap.PropertyOrigin[k]))
	}
	names := sortedFeatureNames(snap.Doc.Features)
	for _, n := range names {
		f := snap.Doc.Features[n]
		al := make([]string, 0, len(f.AllowList))
		for s := range f.AllowList {
			al = append(al, s)
		}
		sortStrings2(al)
		resp.Features = append(resp.Features, FeatureView{
			Name: n, Enabled: f.Enabled, Percentage: f.Percentage,
			AllowList: al, Origin: snap.FeatureOrigin[n],
		})
	}
	return resp
}

func toPropertyView(key string, prop config.Property, origin string) PropertyView {
	v := PropertyView{
		Key: key, Type: string(prop.Value.Kind),
		Value: prop.Value.Display(), Version: prop.Version, Origin: origin,
	}
	if prop.Value.Kind == config.KindSensitive {
		v.Masked = true
	}
	return v
}

func writeError(w http.ResponseWriter, status int, version int64, err error) {
	writeJSON(w, status, errorResponse{
		Error: err.Error(), Kind: string(errs.Classify(err)), Version: version,
	})
}

// writeKindError maps classified errors to HTTP status codes.
func writeKindError(w http.ResponseWriter, version int64, err error) {
	status := http.StatusInternalServerError
	switch errs.Classify(err) {
	case errs.KindValidation, errs.KindTypeMismatch, errs.KindMissingValue:
		status = http.StatusBadRequest
	case errs.KindNotFound, errs.KindRollbackFailed:
		status = http.StatusNotFound
	case errs.KindStaleVersion, errs.KindConflict:
		status = http.StatusConflict
	case errs.KindForbidden:
		status = http.StatusForbidden
	case errs.KindUnauthorized:
		status = http.StatusUnauthorized
	case errs.KindPersistFailed:
		// The commit was aborted before publication; clients may retry.
		status = http.StatusServiceUnavailable
	}
	writeError(w, status, version, err)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

var _ = strconv.Itoa
var _ flag.Decision
