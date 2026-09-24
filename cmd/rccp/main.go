// Command rccp runs a local runtime-config control plane: it loads JSON
// config sources, serves read/flag/admin APIs and hot-reloads files.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/lisama569/runtime-config-control-plane-go/internal/audit"
	"github.com/lisama569/runtime-config-control-plane-go/internal/controlplane"
	"github.com/lisama569/runtime-config-control-plane-go/internal/httpapi"
	"github.com/lisama569/runtime-config-control-plane-go/internal/source"
)

func main() {
	var (
		addr      = flag.String("addr", "127.0.0.1:8080", "listen address")
		srcList   = flag.String("sources", "", "comma separated config files, first has highest priority")
		poll      = flag.Duration("poll", time.Second, "file watch poll interval")
		auditPath = flag.String("audit-log", "", "optional JSONL audit log file")
	)
	flag.Parse()

	var sources []source.Source
	for i, path := range strings.Split(*srcList, ",") {
		path = strings.TrimSpace(path)
		if path == "" {
			continue
		}
		fs, err := source.NewFileSource(fmt.Sprintf("file:%s", path), path)
		if err != nil {
			log.Fatalf("source %d: %v", i, err)
		}
		sources = append(sources, fs)
	}
	if len(sources) == 0 {
		log.Println("warning: no -sources given; starting with empty config")
	}

	var sink audit.Sink
	if *auditPath != "" {
		fs, err := audit.NewFileSink(*auditPath)
		if err != nil {
			log.Fatalf("audit sink: %v", err)
		}
		defer fs.Close()
		sink = fs
	}
	logger := audit.New(4096, sink)
	plane := controlplane.New(controlplane.Options{Sources: sources, Audit: logger})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if len(sources) > 0 {
		watcher := source.NewWatcher(*poll, sources...)
		go func() {
			_ = watcher.Run(ctx, func(ev source.ChangeEvent) {
				if ev.Err != nil {
					logger.Record("watcher", "config.reload", audit.OutcomeReject,
						audit.WithReason("reload failed: "+ev.Err.Error()))
					log.Printf("hot reload rejected: %v", ev.Err)
					return
				}
				snap, err := plane.ReloadLatest(ctx, "watcher", "file change detected")
				if err != nil {
					log.Printf("hot reload rejected: %v", err)
					return
				}
				log.Printf("hot reload committed: version=%d", snap.Version)
			})
		}()
	}

	srv := &http.Server{Addr: *addr, Handler: httpapi.NewServer(plane).Handler()}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	log.Printf("rccp listening on %s (version=%d)", *addr, plane.CurrentVersion())
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("serve: %v", err)
	}
}
