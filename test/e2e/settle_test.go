package e2e

import (
	"encoding/json"
	"net/http"
	"slices"
	"testing"
	"time"
)

// These tests run with the harness's test settings: a quiet window of 1s, a
// max duration of 5s and a settle grace of 200ms.

func TestASilentCameraSettlesQuietAndIsAnnouncedOnce(t *testing.T) {
	p := startReady(t)
	s := newScenario(t)

	s.publish(s.event(1, "warning", 0), s.event(2, "critical", 200*ms), s.event(3, "info", 400*ms))
	published := time.Now()
	s.waitDrained()

	episodes := s.episodes(p)
	if len(episodes) != 1 || episodes[0].Status != "open" {
		t.Fatalf("episodes right after the events = %+v, want one open episode", episodes)
	}
	id := episodes[0].ID
	var settled episodeJSON
	waitFor(t, 5*time.Second, "settling", func() bool {
		settled, _ = s.episode(p, id)
		return settled.Status == "settled"
	})
	// Silence is measured from the arrival of the latest event, which came
	// after `published`, so nothing may settle sooner than a quiet window.
	if waited := time.Since(published); waited < time.Second {
		t.Errorf("settled %v after the last event was published, before the 1s quiet window", waited)
	}
	if settled.CloseReason == nil || *settled.CloseReason != "quiet" || settled.SettledAt == nil {
		t.Errorf("close_reason %v, settled_at %v; want quiet and a time", settled.CloseReason, settled.SettledAt)
	}

	time.Sleep(time.Second) // room for a second announcement, which must not come
	got := s.announcements()
	if len(got) != 1 {
		t.Fatalf("got %d announcements, want 1: %+v", len(got), got)
	}
	a := got[0]
	if a.Subject != "episode.settled."+s.site+".cam-1" || a.MsgID != "settled:"+id {
		t.Errorf("announcement on %s with id %s; want episode.settled.%s.cam-1 and settled:%s", a.Subject, a.MsgID, s.site, id)
	}
	// The episode object with episode_id in place of id, and nothing else.
	var api map[string]json.RawMessage
	var detail struct {
		Episode json.RawMessage `json:"episode"`
	}
	p.getJSON("/episodes/"+id, http.StatusOK, &detail)
	if err := json.Unmarshal(detail.Episode, &api); err != nil {
		t.Fatal(err)
	}
	api["episode_id"] = api["id"]
	delete(api, "id")
	assertKeys(t, "announcement", a.Fields, keysOf(api)...)
	for k, v := range api {
		if string(a.Fields[k]) != string(v) {
			t.Errorf("announcement %s = %s, API says %s", k, a.Fields[k], v)
		}
	}
}

func TestABusyCameraSettlesEachChapter(t *testing.T) {
	p := startReady(t)
	s := newScenario(t)
	time.Sleep(time.Second) // past the restart guard, so a due episode settles at the next sweep

	// Chapter one: ten events half a second apart in device time, published
	// at once. The next event, at 5s, is on its boundary.
	for i := range 10 {
		s.publish(s.event(i, "info", time.Duration(i)*500*ms))
	}
	// Chapter two keeps the camera busy: an event every 200ms of real time,
	// well inside the quiet window, for 1.2s.
	for i := 10; i < 16; i++ {
		s.publish(s.event(i, "info", time.Duration(i)*500*ms))
		time.Sleep(200 * time.Millisecond)
	}

	// By now the first chapter, which ended by device time when the 5s event
	// arrived, has settled within its grace, while the second is still open.
	settled := s.list(p, "&status=settled")
	open := s.list(p, "&status=open")
	if len(settled) != 1 || len(open) != 1 {
		t.Fatalf("while the camera is busy: settled %v, open %v; want one of each", settled, open)
	}
	episodes := s.episodes(p)
	first, second := episodes[1], episodes[0]
	if first.Status != "settled" || first.CloseReason == nil || *first.CloseReason != "max_duration" || first.EventCount != 10 {
		t.Errorf("first chapter %+v; want settled as max_duration with 10 events", first)
	}

	waitFor(t, 5*time.Second, "the second chapter settling", func() bool {
		second, _ = s.episode(p, second.ID)
		return second.Status == "settled"
	})
	if *second.CloseReason != "quiet" || second.EventCount != 6 {
		t.Errorf("second chapter: close reason %s, %d events; want quiet with 6", *second.CloseReason, second.EventCount)
	}

	got := s.announcements()
	var reasons []string
	for _, a := range got {
		var reason string
		_ = json.Unmarshal(a.Fields["close_reason"], &reason)
		reasons = append(reasons, reason)
	}
	if !slices.Equal(reasons, []string{"max_duration", "quiet"}) {
		t.Errorf("announcements' close reasons = %v, want [max_duration quiet]", reasons)
	}
}

func keysOf(m map[string]json.RawMessage) []string {
	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
