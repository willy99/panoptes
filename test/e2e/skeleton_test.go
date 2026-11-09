package e2e

import (
	"net/http"
	"syscall"
	"testing"
	"time"
)

func TestHealthzAnswersWhileRunning(t *testing.T) {
	p := startReady(t)

	status, body := p.get("/healthz")
	if status != http.StatusOK {
		t.Fatalf("GET /healthz = %d %s, want 200", status, body)
	}
}

func TestInvalidConfigurationStopsTheProcess(t *testing.T) {
	p := start(t, "EPISODE_QUIET_WINDOW=abc")

	if code := p.wait(10 * time.Second); code == 0 {
		t.Fatal("exit code = 0, want non-zero for an invalid EPISODE_QUIET_WINDOW")
	}
	if !p.output.contains("EPISODE_QUIET_WINDOW") {
		t.Fatalf("output does not name EPISODE_QUIET_WINDOW:\n%s", p.output.String())
	}
}

func TestSIGTERMExitsCleanly(t *testing.T) {
	p := startReady(t)

	p.signal(syscall.SIGTERM)

	if code := p.wait(25 * time.Second); code != 0 {
		t.Fatalf("exit code after SIGTERM = %d, want 0", code)
	}
}
