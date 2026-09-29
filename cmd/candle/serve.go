package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jayrajadeja/candle/logsource"
	"github.com/jayrajadeja/candle/server"
)

// runServeCmd parses the `serve` subcommand flags and runs the HTTP server until
// an interrupt/termination signal, then shuts down gracefully.
func runServeCmd(args []string, stderr io.Writer) int {
	fs := flag.NewFlagSet("candle serve", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dir := fs.String("dir", "data", "directory of tickstore per-symbol .log files")
	addr := fs.String("addr", "127.0.0.1:8138", "listen address")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runServe(*dir, *addr, ctx, nil); err != nil {
		fmt.Fprintf(stderr, "candle serve: %v\n", err)
		return 1
	}
	return 0
}

// runServe starts a read-only HTTP candle server over the logs at dir and blocks
// until ctx is cancelled, then shuts down gracefully. When ready is non-nil, the
// bound listen address is sent on it once listening (useful with :0 in tests).
func runServe(dir, addr string, ctx context.Context, ready chan<- string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: server.Handler(logsource.NewIncremental(dir))}
	if ready != nil {
		ready <- ln.Addr().String()
	}
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()
	fmt.Fprintf(os.Stderr, "candle serve: listening on %s (dir=%s)\n", ln.Addr(), dir)
	if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}
