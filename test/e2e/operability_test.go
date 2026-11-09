package e2e

import (
	"net/http"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestMetricsCoverThePipeline(t *testing.T) {
	// A database of its own: the counters are per process, and in a shared
	// database this instance would also settle other tests' open episodes.
	p := startReady(t, "DATABASE_URL="+freshDatabase(t))
	s := newScenario(t)

	first := s.event(1, "info", 0)
	s.publish(first, s.event(2, "info", 200*ms), first) // the third is a duplicate
	s.publishRaw("analytics.event.v1."+s.site+".cam-1.tamper", []byte("not json"))
	s.waitDrained()
	id := s.episodes(p)[0].ID
	s.waitSettled(p, id)
	s.publish(s.event(3, "info", 400*ms)) // late: a correction
	s.waitDrained()
	waitFor(t, 5*time.Second, "the outbox to empty", func() bool { return p.metric("panoptes_outbox_pending") == 0 })

	for series, want := range map[string]float64{
		"episoder_invalid_events_total":                                1,
		`panoptes_ingested_events_total{result="new"}`:                 3,
		`panoptes_ingested_events_total{result="duplicate"}`:           1,
		`panoptes_episodes_settled_total{close_reason="quiet"}`:        1,
		`panoptes_episodes_settled_total{close_reason="max_duration"}`: 0,
		"panoptes_episode_corrections_total":                           1,
		"panoptes_outbox_pending":                                      0,
		"panoptes_publish_failures_total":                              0,
		"panoptes_ingest_duration_seconds_count":                       4,
	} {
		if got := p.metric(series); got != want {
			t.Errorf("%s = %v, want %v", series, got, want)
		}
	}
}

func TestSIGTERMUnderLoadFinishesWorkInFlight(t *testing.T) {
	p := startReady(t)
	s := newScenario(t)

	// Load from one goroutine, while the test sends SIGTERM.
	stop := make(chan struct{})
	var load sync.WaitGroup
	load.Go(func() {
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			s.publish(s.event(i, "info", time.Duration(i)*10*ms))
			time.Sleep(2 * time.Millisecond)
		}
	})
	time.Sleep(300 * time.Millisecond)

	p.signal(syscall.SIGTERM)
	signalled := time.Now()
	// Once the process has taken the signal in, /readyz never says ready again.
	waitFor(t, 5*time.Second, "the shutdown to begin", func() bool { return p.output.contains("shutting down") })
	for {
		select {
		case <-p.done:
		default:
			if status, _ := p.get("/readyz"); status == http.StatusOK {
				t.Errorf("/readyz returned 200 after SIGTERM")
			}
			time.Sleep(5 * time.Millisecond)
			continue
		}
		break
	}
	if code := p.wait(time.Second); code != 0 || time.Since(signalled) > 20*time.Second {
		t.Errorf("exit code %d after %v; want 0 within 20s", code, time.Since(signalled))
	}
	close(stop)
	load.Wait()

	// A new instance reads what the old one left. Every message was delivered
	// exactly once in all: the old instance let its open pulls run out and
	// acknowledged everything they brought, so nothing waits out AckWait.
	next := startReady(t)
	s.waitDrained()
	s.waitAllSettled(next)
	s.checkInvariants(next)
}
