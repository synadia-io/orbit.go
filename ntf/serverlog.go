// Copyright 2026 Synadia Communications Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ntf

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"

	srvlog "github.com/nats-io/nats-server/v2/logger"
	"github.com/nats-io/nats-server/v2/server"
)

const (
	// serverStartTimeout is how long a managed server has to accept connections.
	serverStartTimeout = 10 * time.Second

	// serverStartPoll is how often a starting server is checked for readiness or a
	// fatal error, so a server that failed to bind is caught at once rather than
	// after serverStartTimeout.
	serverStartPoll = 50 * time.Millisecond
)

// fatalGuard is a managed server's logger. It passes every call to the logger
// nats-server would have built for the server, except Fatalf: nats-server's file
// and stderr loggers implement that with log.Logger.Fatalf, which calls os.Exit(1)
// and would take the whole service down with one managed server. fatalGuard logs
// the line to the server's own log and to the service log instead, records it,
// and returns, leaving the start path to shut the server down.
type fatalGuard struct {
	server.Logger

	name string
	log  *slog.Logger

	mu  sync.Mutex
	err error
}

// Fatalf logs a fatal error without exiting and records the first one. A failure
// to bind a listener is recorded as ErrServerListen, anything else as
// ErrServerFatal.
func (g *fatalGuard) Fatalf(format string, v ...any) {
	msg := fmt.Sprintf(format, v...)
	g.Logger.Errorf("Fatal: %s", msg)
	g.log.Error("Managed server reported a fatal error", "server", g.name, "err", msg)

	cause := ErrServerFatal
	if isListenFailure(v) {
		cause = ErrServerListen
	}

	g.mu.Lock()
	if g.err == nil {
		g.err = fmt.Errorf("%w: %s", cause, msg)
	}
	g.mu.Unlock()
}

// fatal returns the first fatal error the server reported, or nil.
func (g *fatalGuard) fatal() error {
	g.mu.Lock()
	defer g.mu.Unlock()

	return g.err
}

// Close closes the wrapped logger when it holds a resource such as a log file.
// nats-server calls it when a reload replaces the logger.
func (g *fatalGuard) Close() error {
	c, ok := g.Logger.(io.Closer)
	if !ok {
		return nil
	}

	return c.Close()
}

// isListenFailure reports whether any argument of a log call is a failure to bind
// a listener, which is how nats-server reports a port it could not listen on.
func isListenFailure(args []any) bool {
	for _, arg := range args {
		err, ok := arg.(error)
		if !ok {
			continue
		}

		var opErr *net.OpError
		if errors.As(err, &opErr) && opErr.Op == "listen" {
			return true
		}
	}

	return false
}

// guardServerLogger installs a fatalGuard as srv's logger, wrapping the logger
// srv.ConfigureLogger would install, built from opts the same way and with the
// same debug and trace settings. ConfigureLogger cannot be called first and its
// logger wrapped afterwards: SetLoggerV2 closes the logger it replaces, log file
// included. nats-server closes only its own logger type on shutdown, so the
// wrapped logger is closed here once srv has shut down.
func guardServerLogger(srv *server.Server, opts *server.Options, log *slog.Logger) *fatalGuard {
	guard := &fatalGuard{Logger: newServerLogger(opts), name: srv.Name(), log: log}
	srv.SetLoggerV2(guard, opts.Debug, opts.Trace, opts.TraceVerbose)

	go func() {
		srv.WaitForShutdown()
		guard.Close()
	}()

	return guard
}

// newServerLogger builds the logger nats-server's ConfigureLogger builds for opts.
func newServerLogger(opts *server.Options) server.Logger {
	switch {
	case opts.LogFile != "":
		l := srvlog.NewFileLogger(opts.LogFile, opts.Logtime, opts.Debug, opts.Trace, true, srvlog.LogUTC(opts.LogtimeUTC))
		if opts.LogSizeLimit > 0 {
			l.SetSizeLimit(opts.LogSizeLimit)
		}
		if opts.LogMaxFiles > 0 {
			l.SetMaxNumFiles(int(opts.LogMaxFiles))
		}
		return l

	case opts.RemoteSyslog != "":
		return srvlog.NewRemoteSysLogger(opts.RemoteSyslog, opts.Debug, opts.Trace)

	case opts.Syslog:
		return srvlog.NewSysLogger(opts.Debug, opts.Trace)

	default:
		// Color only when stderr is a terminal, as ConfigureLogger decides.
		colors := false
		stat, err := os.Stderr.Stat()
		if err == nil && stat.Mode()&os.ModeCharDevice != 0 {
			colors = true
		}
		return srvlog.NewStdLogger(opts.Logtime, opts.Debug, opts.Trace, colors, true, srvlog.LogUTC(opts.LogtimeUTC))
	}
}

// waitForStart waits for srv to accept connections. It returns the fatal error
// guard recorded as soon as there is one, even when the server also became ready:
// a fatal error that did not stop startup still leaves the server broken.
func waitForStart(srv *server.Server, guard *fatalGuard) error {
	deadline := time.Now().Add(serverStartTimeout)

	for {
		ready := srv.ReadyForConnections(serverStartPoll)

		err := guard.fatal()
		if err != nil {
			return err
		}
		if ready {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("not ready for connections after %v", serverStartTimeout)
		}
	}
}
