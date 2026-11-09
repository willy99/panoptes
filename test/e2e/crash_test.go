package e2e

import (
	"encoding/json"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// These tests kill instances and start new ones, then check the invariants
// that must hold after any kill. They run with the harness's test settings:
// a quiet window of 1s, a max duration of 5s and a settle grace of 200ms.

// shortAckWait recreates the input stream with an episoder consumer whose
// AckWait is 2s instead of 30s, so that a message a killed instance held comes
// back quickly. The service uses an existing consumer as it is. The stream
// is deleted afterwards, so later tests get the service's own.
func shortAckWait(t *testing.T) {
	t.Helper()
	js := jetStream(t)
	deleteStreams(t, js, "ANALYTICS_EVENTS")
	_, err := js.CreateStream(ctx(t), jetstream.StreamConfig{
		Name: "ANALYTICS_EVENTS", Subjects: []string{"analytics.event.v1.>"},
		Retention: jetstream.WorkQueuePolicy, Storage: jetstream.FileStorage,
	})
	if err != nil {
		t.Fatalf("create ANALYTICS_EVENTS: %v", err)
	}
	_, err = js.CreateConsumer(ctx(t), "ANALYTICS_EVENTS", jetstream.ConsumerConfig{
		Durable: "episoder", AckPolicy: jetstream.AckExplicitPolicy, AckWait: 2 * time.Second, MaxDeliver: 10,
	})
	if err != nil {
		t.Fatalf("create consumer episoder: %v", err)
	}
	t.Cleanup(func() { deleteStreams(t, js, "ANALYTICS_EVENTS") })
}

func TestEachCrashPointLosesNothingAndAnnouncesOnce(t *testing.T) {
	for _, point := range []string{
		"ingest.before_commit", "ingest.after_commit", "settle.before_commit",
		"relay.before_publish", "relay.after_publish",
	} {
		t.Run(point, func(t *testing.T) {
			shortAckWait(t)
			doomed := start(t, "PANOPTES_CRASH_AT="+point)
			doomed.waitReady()
			s := newScenario(t)
			s.redelivered = true

			s.publish(s.event(1, "info", 0), s.event(2, "warning", 300*ms), s.event(3, "info", 600*ms))
			if code := doomed.wait(10 * time.Second); code != 3 {
				t.Fatalf("exit code %d at %s, want 3", code, point)
			}
			if !doomed.output.contains(point) {
				t.Errorf("the output does not name the crash point:\n%s", doomed.output.String())
			}

			p := startReady(t)
			s.waitDrained()
			s.waitAllSettled(p)
			s.checkInvariants(p)
			if point == "relay.after_publish" {
				waitFor(t, 5*time.Second, "the outbox to empty", func() bool { return p.metric("panoptes_outbox_pending") == 0 })
			}
		})
	}
}

func TestNothingSettlesWithinAQuietWindowOfStart(t *testing.T) {
	p := startReady(t, "EPISODE_QUIET_WINDOW=2s")
	s := newScenario(t)
	s.publish(s.event(1, "info", 0))
	s.waitDrained()
	id := s.episodes(p)[0].ID
	p.kill()

	time.Sleep(2500 * time.Millisecond) // the episode is now due by silence
	p = startReady(t, "EPISODE_QUIET_WINDOW=2s")
	started := time.Now()
	for time.Since(started) < 1500*time.Millisecond {
		if ep, _ := s.episode(p, id); ep.Status != "open" {
			t.Fatalf("settled %v after start, within the 2s restart guard", time.Since(started))
		}
		time.Sleep(100 * time.Millisecond)
	}
	s.waitSettled(p, id)
}

func TestEventsPublishedWhileDownJoinTheirOpenEpisode(t *testing.T) {
	p := startReady(t)
	s := newScenario(t)
	s.publish(s.event(1, "info", 0), s.event(2, "info", 300*ms))
	s.waitDrained()
	id := s.episodes(p)[0].ID
	p.kill()

	// Longer than the quiet window, so the episode is silent by then; its
	// next events wait in the stream.
	time.Sleep(1500 * time.Millisecond)
	s.publish(s.event(3, "info", 600*ms), s.event(4, "critical", 900*ms))
	p = startReady(t)
	s.waitDrained()

	ep := s.waitSettled(p, id)
	if ep.EventCount != 4 || ep.PeakSeverity != "critical" {
		t.Errorf("episode after the restart: %d events, peak %s; want 4 and critical", ep.EventCount, ep.PeakSeverity)
	}
	s.checkInvariants(p)
	for _, a := range s.announcements() {
		if _, ok := a.Fields["revision"]; ok {
			t.Errorf("a correction was announced: the episode settled before its queued events were read")
		}
	}
}

// waitAllSettled waits until every episode of the scenario has settled.
func (s *scenario) waitAllSettled(p *proc) {
	s.t.Helper()
	waitFor(s.t, 10*time.Second, "every episode settling", func() bool {
		var body listJSON
		p.getJSON("/episodes?status=open&site_id="+s.site, http.StatusOK, &body)
		return len(body.Episodes) == 0
	})
}

// checkInvariants checks what must hold after any kill:
//   - every valid event published is stored once, in exactly one episode;
//   - each episode's aggregates match its own list of events;
//   - the episode stream holds exactly one settled message per settled
//     episode, and none for an open one.
func (s *scenario) checkInvariants(p *proc) {
	s.t.Helper()
	// Every settled episode's announcement has left the outbox.
	waitFor(s.t, 10*time.Second, "the outbox to empty", func() bool { return p.metric("panoptes_outbox_pending") == 0 })
	var episodes []episodeJSON
	cursor := ""
	for {
		var body listJSON
		p.getJSON("/episodes?limit=200&site_id="+s.site+"&cursor="+url.QueryEscape(cursor), http.StatusOK, &body)
		episodes = append(episodes, body.Episodes...)
		if body.NextCursor == "" {
			break
		}
		cursor = body.NextCursor
	}

	rank := map[string]int{"info": 1, "notice": 2, "warning": 3, "critical": 4}
	seen := map[string]int{}
	for _, e := range episodes {
		ep, events := s.episode(p, e.ID)
		peak := ""
		for _, ev := range events {
			seen[ev.EventID]++
			if rank[ev.Severity] > rank[peak] {
				peak = ev.Severity
			}
		}
		if len(events) == 0 || ep.EventCount != len(events) || ep.FirstSeen != events[0].OccurredAt ||
			ep.LastSeen != events[len(events)-1].OccurredAt || ep.PeakSeverity != peak {
			s.t.Errorf("episode %s: aggregates %+v do not match its %d events", e.ID, ep, len(events))
		}
	}
	for id := range s.valid {
		if seen[id] != 1 {
			s.t.Errorf("event %s is stored %d times, want once", id, seen[id])
		}
	}
	for id, n := range seen {
		if !s.valid[id] {
			s.t.Errorf("event %s is stored %d times but was never published as valid", id, n)
		}
	}

	settledMessages := map[string]int{}
	for _, a := range s.announcements() {
		if _, ok := a.Fields["revision"]; ok {
			continue
		}
		var id string
		_ = json.Unmarshal(a.Fields["episode_id"], &id)
		settledMessages[id]++
	}
	for _, e := range episodes {
		want := 0
		if e.Status == "settled" {
			want = 1
		}
		if got := settledMessages[e.ID]; got != want {
			s.t.Errorf("episode %s (%s) has %d settled messages, want %d", e.ID, e.Status, got, want)
		}
		delete(settledMessages, e.ID)
	}
	for id, n := range settledMessages {
		s.t.Errorf("%d settled messages for %s, which is not an episode", n, id)
	}
}
