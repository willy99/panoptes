// Package e2e drives Panoptes from the outside, the way the grading suite does.
// It builds the binary, starts it as child processes and talks to them only
// through NATS, HTTP and /metrics. It needs NATS_URL and DATABASE_URL; without
// them it skips itself, so `go test ./...` on a laptop runs only the unit tests.
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// binary is the path of the Panoptes binary built once for the whole run.
var binary string

// skipReason is set when there is no NATS or Postgres to test against.
var skipReason string

// testSettings are the short windows from the spec's Seam A: device times in
// payloads are arbitrary, so only the settle waits depend on these.
var testSettings = []string{
	"EPISODE_QUIET_WINDOW=1s",
	"EPISODE_MAX_DURATION=5s",
	"EPISODE_SETTLE_GRACE=200ms",
}

// openSettings make the windows long enough that nothing settles while a test
// reads open episodes: a quiet window of a minute, and a settle grace of an
// hour for episodes that have ended by device time.
var openSettings = []string{
	"EPISODE_QUIET_WINDOW=1m",
	"EPISODE_MAX_DURATION=5m",
	"EPISODE_SETTLE_GRACE=1h",
}

func TestMain(m *testing.M) {
	if os.Getenv("NATS_URL") == "" || os.Getenv("DATABASE_URL") == "" {
		skipReason = "NATS_URL or DATABASE_URL is not set; make test sets both"
		os.Exit(m.Run())
	}
	dir, err := os.MkdirTemp("", "panoptes-e2e")
	if err != nil {
		fmt.Println("e2e:", err)
		os.Exit(1)
	}
	binary = filepath.Join(dir, "panoptes")
	build := exec.Command("go", "build", "-o", binary, "panoptes/cmd/panoptes")
	build.Stdout, build.Stderr = os.Stdout, os.Stderr
	if err := build.Run(); err != nil {
		fmt.Println("e2e: building panoptes failed:", err)
		os.Exit(1)
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

// proc is one running Panoptes instance.
type proc struct {
	t      *testing.T
	cmd    *exec.Cmd
	base   string // http://127.0.0.1:<port>
	output *lockedBuffer
	done   chan struct{} // closed when the process has exited
	code   int           // exit code, valid after done is closed
}

// requireStack skips the test when there is no NATS or Postgres to test against.
func requireStack(t *testing.T) {
	t.Helper()
	if skipReason != "" {
		t.Skip(skipReason)
	}
}

// start launches an instance with the test's environment and test settings,
// plus extra KEY=VALUE pairs, which override both. It does not wait for readiness.
func start(t *testing.T, extra ...string) *proc {
	t.Helper()
	requireStack(t)
	port := freePort(t)
	p := &proc{
		t:      t,
		base:   "http://127.0.0.1:" + port,
		output: &lockedBuffer{},
		done:   make(chan struct{}),
	}
	env := append(os.Environ(), testSettings...)
	env = append(env, "HTTP_ADDR=127.0.0.1:"+port)
	p.cmd = exec.Command(binary)
	p.cmd.Env = append(env, extra...) // later entries win over earlier ones
	p.cmd.Stdout, p.cmd.Stderr = p.output, p.output
	if err := p.cmd.Start(); err != nil {
		t.Fatalf("start panoptes: %v", err)
	}
	go func() {
		err := p.cmd.Wait()
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			p.code = exit.ExitCode()
		}
		close(p.done)
	}()
	t.Cleanup(func() {
		p.kill()
		if t.Failed() {
			t.Logf("panoptes output (%s):\n%s", p.base, p.output.String())
		}
	})
	return p
}

// startReady launches an instance and waits until /readyz returns 200.
func startReady(t *testing.T, extra ...string) *proc {
	t.Helper()
	p := start(t, extra...)
	p.waitReady()
	return p
}

func (p *proc) waitReady() {
	p.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-p.done:
			p.t.Fatalf("panoptes exited with code %d before it became ready", p.code)
		default:
		}
		if status, _ := p.get("/readyz"); status == http.StatusOK {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	p.t.Fatal("panoptes did not become ready within 20s")
}

// get performs a GET and returns the status and body; status 0 means no answer.
func (p *proc) get(path string) (int, string) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, p.base+path, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, ""
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

// getJSON performs a GET, checks the status and decodes the body into v.
func (p *proc) getJSON(path string, wantStatus int, v any) {
	p.t.Helper()
	status, body := p.get(path)
	if status != wantStatus {
		p.t.Fatalf("GET %s: status %d, want %d; body %s", path, status, wantStatus, body)
	}
	if err := json.Unmarshal([]byte(body), v); err != nil {
		p.t.Fatalf("GET %s: decode %s: %v", path, body, err)
	}
}

// signal sends sig to the process.
func (p *proc) signal(sig syscall.Signal) {
	p.t.Helper()
	if err := p.cmd.Process.Signal(sig); err != nil {
		p.t.Fatalf("send %v: %v", sig, err)
	}
}

// wait blocks until the process exits and returns its exit code.
func (p *proc) wait(timeout time.Duration) int {
	p.t.Helper()
	select {
	case <-p.done:
		return p.code
	case <-time.After(timeout):
		p.t.Fatalf("panoptes did not exit within %v", timeout)
		return -1
	}
}

// kill stops the process with SIGKILL if it is still running.
func (p *proc) kill() {
	select {
	case <-p.done:
	default:
		_ = p.cmd.Process.Kill()
		<-p.done
	}
}

func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find a free port: %v", err)
	}
	defer ln.Close()
	_, port, _ := net.SplitHostPort(ln.Addr().String())
	return port
}

// lockedBuffer collects a child process's output from two goroutines.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *lockedBuffer) contains(s string) bool {
	return strings.Contains(b.String(), s)
}
