// Command rccp runs the local runtime configuration control plane.
//
// Usage example:
//
//	rccp -source base=1:testdata/base.json -source override=0:testdata/override.json \
//	      -addr :8080
//
// Each -source has the form name=priority:path (lower priority number wins).
// Demo bearer tokens are seeded unless -tokens-file is given:
//
//	reader-token (reader), writer-token (writer), admin-token (admin).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/lisama569/runtime-config-control-plane-go/internal/auth"
	"github.com/lisama569/runtime-config-control-plane-go/internal/controlplane"
	"github.com/lisama569/runtime-config-control-plane-go/internal/source"
)

type sourceFlag []string

func (s *sourceFlag) String() string { return strings.Join(*s, ",") }
func (s *sourceFlag) Set(v string) error {
	if !strings.Contains(v, "=") || !strings.Contains(v, ":") {
		return errors.New("source must be name=priority:path")
	}
	*s = append(*s, v)
	return nil
}

func main() {
	var srcs sourceFlag
	var addr, tokensFile, persistDir string
	var poll time.Duration
	flag.Var(&srcs, "source", "configuration source name=priority:path (repeatable)")
	flag.StringVar(&addr, "addr", "127.0.0.1:8080", "listen address")
	flag.StringVar(&tokensFile, "tokens-file", "", "JSON file mapping token -> {name, role}")
	flag.StringVar(&persistDir, "persist-dir", "", "data directory for durable state + audit (empty = in-memory only)")
	flag.DurationVar(&poll, "poll", 500*time.Millisecond, "file poll interval")
	flag.Parse()

	if len(srcs) == 0 {
		log.Fatal("at least one -source is required, e.g. -source base=1:testdata/base.json")
	}

	sources := make([]source.Source, 0, len(srcs))
	for _, spec := range srcs {
		name, prio, path, err := parseSource(spec)
		if err != nil {
			log.Fatalf("bad -source %q: %v", spec, err)
		}
		sources = append(sources, source.NewFileSource(name, prio, path))
	}

	tokens, err := loadTokens(tokensFile)
	if err != nil {
		log.Fatalf("tokens: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	plane, err := controlplane.New(ctx, controlplane.Options{
		Sources:      sources,
		Tokens:       tokens,
		PollInterval: poll,
		PersistDir:   persistDir,
	})
	if err != nil {
		log.Fatalf("startup failed (failing closed): %v", err)
	}
	defer plane.Close()

	srv := &http.Server{Addr: addr, Handler: plane.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Printf("rccp listening on %s (version=%d)", addr, plane.Current().Version)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http: %v", err)
		}
	}()
	<-ctx.Done()
	log.Println("shutting down")
	shctx, c := context.WithTimeout(context.Background(), 3*time.Second)
	defer c()
	_ = srv.Shutdown(shctx)
}

func parseSource(spec string) (string, int, string, error) {
	namePart, rest, ok := strings.Cut(spec, "=")
	if !ok {
		return "", 0, "", errors.New("expected name=priority:path")
	}
	prioPart, path, ok := strings.Cut(rest, ":")
	if !ok {
		return "", 0, "", errors.New("expected priority:path after name=")
	}
	prio, err := strconv.Atoi(strings.TrimSpace(prioPart))
	if err != nil {
		return "", 0, "", fmt.Errorf("priority %q is not an int", prioPart)
	}
	return strings.TrimSpace(namePart), prio, path, nil
}

func loadTokens(path string) (map[string]auth.Principal, error) {
	if path == "" {
		return map[string]auth.Principal{
			"reader-token": {Name: "reader-1", Role: auth.RoleReader},
			"writer-token": {Name: "writer-1", Role: auth.RoleWriter},
			"admin-token":  {Name: "admin-1", Role: auth.RoleAdmin},
		}, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var raw map[string]auth.Principal
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}
	return raw, nil
}
