package e2e

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"
)

// invalidCounter is the brief's metric for skipped messages.
const invalidCounter = "episoder_invalid_events_total"

func TestTheInvalidCounterStartsAtZero(t *testing.T) {
	p := startReady(t)
	if got := p.metric(invalidCounter); got != 0 {
		t.Errorf("%s = %v on a fresh instance, want 0", invalidCounter, got)
	}
}

func TestEachInvalidEventIsCountedOnceAndNotStored(t *testing.T) {
	p := startReady(t)
	s := newScenario(t)
	subject := "analytics.event.v1." + s.site + ".cam-1.person_detected"

	// valid returns a fresh valid payload as a map, for a case to break.
	n := 0
	valid := func() map[string]any {
		n++
		return map[string]any{
			"event_id": fmt.Sprintf("%s-bad-%03d", s.site, n), "site_id": s.site, "camera_id": "cam-1",
			"kind": "person_detected", "severity": "info", "occurred_at": at(0),
			"attributes": map[string]any{"confidence": 0.5},
		}
	}
	type invalidCase struct {
		name    string
		payload []byte
	}
	var cases []invalidCase
	add := func(name string, change func(map[string]any)) {
		m := valid()
		change(m)
		b, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		cases = append(cases, invalidCase{name, b})
	}

	cases = append(cases, invalidCase{"not JSON", []byte("not json")})
	cases = append(cases, invalidCase{"a JSON array", []byte(`[1, 2]`)})
	for _, field := range []string{"event_id", "site_id", "camera_id", "kind", "severity", "occurred_at"} {
		add(field+" missing", func(m map[string]any) { delete(m, field) })
		add(field+" null", func(m map[string]any) { m[field] = nil })
		add(field+" empty", func(m map[string]any) { m[field] = "" })
		add(field+" a number", func(m map[string]any) { m[field] = 7 })
	}
	add("unknown kind", func(m map[string]any) { m["kind"] = "car_detected" })
	add("kind in another case", func(m map[string]any) { m["kind"] = "Tamper" })
	add("unknown severity", func(m map[string]any) { m["severity"] = "fatal" })
	add("time without a zone", func(m map[string]any) { m["occurred_at"] = "2026-08-04T10:00:00" })
	add("time with a space", func(m map[string]any) { m["occurred_at"] = "2026-08-04 10:00:00Z" })
	add("time with a comma fraction", func(m map[string]any) { m["occurred_at"] = "2026-08-04T10:00:00,123Z" })
	add("a dot in site_id", func(m map[string]any) { m["site_id"] = s.site + ".x" })
	add("a dot in camera_id", func(m map[string]any) { m["camera_id"] = "cam.1" })
	add("a space in camera_id", func(m map[string]any) { m["camera_id"] = "cam 1" })
	add("a wildcard in site_id", func(m map[string]any) { m["site_id"] = s.site + "*" })
	add("U+0000 in event_id", func(m map[string]any) { m["event_id"] = s.site + "-nul\u0000" })
	add("event_id over 255 bytes", func(m map[string]any) { m["event_id"] = s.site + strings.Repeat("x", 255) })
	cases = append(cases, invalidCase{"invalid UTF-8", []byte(`{"event_id":"` + s.site + `-utf","site_id":"` + s.site +
		`","camera_id":"cam-1","kind":"tamper","severity":"info","occurred_at":"2026-08-04T10:00:00Z","attributes":{"x":"` + "\xff" + `"}}`)})

	for _, c := range cases {
		before := p.metric(invalidCounter)
		s.publishRaw(subject, c.payload)
		s.waitDrained()
		if got := p.metric(invalidCounter); got != before+1 {
			t.Errorf("%s: %s went from %v to %v, want one more", c.name, invalidCounter, before, got)
		}
	}
	if episodes := s.episodes(p); len(episodes) != 0 {
		t.Errorf("invalid events were stored: %+v", episodes)
	}

	// A valid event after all of them is stored, and nothing comes back later
	// to raise the counter again.
	counted := p.metric(invalidCounter)
	s.publish(s.event(1, "info", 0))
	s.waitDrained()
	if episodes := s.episodes(p); len(episodes) != 1 || episodes[0].EventCount != 1 {
		t.Errorf("the valid event after the invalid ones: episodes %+v, want one with one event", episodes)
	}
	time.Sleep(time.Second)
	if got := p.metric(invalidCounter); got != counted {
		t.Errorf("%s grew from %v to %v after the stream drained", invalidCounter, counted, got)
	}
}

func TestEdgeCasesThatAreStillValid(t *testing.T) {
	p := startReady(t, openSettings...)
	s := newScenario(t)

	lowercase := s.event(1, "info", 0)
	lowercase.OccurredAt = "2026-08-04t10:00:00.500z"
	offset := s.event(2, "info", 0)
	offset.OccurredAt = "2026-08-04T12:00:01+02:00" // 10:00:01 UTC
	s.publish(lowercase, offset)
	s.waitDrained()

	episodes := s.episodes(p)
	if len(episodes) != 1 {
		t.Fatalf("got %d episodes, want 1: %+v", len(episodes), episodes)
	}
	if e := episodes[0]; e.FirstSeen != at(500*ms) || e.LastSeen != at(time.Second) || e.EventCount != 2 {
		t.Errorf("first seen %s, last seen %s, event count %d; want %s, %s, 2", e.FirstSeen, e.LastSeen, e.EventCount, at(500*ms), at(time.Second))
	}
	if got := p.metric(invalidCounter); got != 0 {
		t.Errorf("%s = %v, want 0", invalidCounter, got)
	}
}

func TestThePayloadWinsOverTheSubject(t *testing.T) {
	p := startReady(t, openSettings...)
	s := newScenario(t)

	e := s.event(1, "warning", 0)
	payload, _ := json.Marshal(e)
	s.publishRaw("analytics.event.v1."+s.site+".cam-9.tamper", payload)
	s.waitDrained()

	episodes := s.episodes(p)
	if len(episodes) != 1 || episodes[0].CameraID != "cam-1" || episodes[0].Kind != "person_detected" {
		t.Errorf("episodes %+v, want one at cam-1 of kind person_detected, as the payload says", episodes)
	}
}

// metric reads one unlabelled metric from /metrics.
func (p *proc) metric(name string) float64 {
	p.t.Helper()
	status, body := p.get("/metrics")
	if status != 200 {
		p.t.Fatalf("GET /metrics: status %d", status)
	}
	for _, line := range strings.Split(body, "\n") {
		if value, ok := strings.CutPrefix(line, name+" "); ok {
			v, err := strconv.ParseFloat(value, 64)
			if err != nil {
				p.t.Fatalf("metric %s: %v", line, err)
			}
			return v
		}
	}
	p.t.Fatalf("metric %s is not on /metrics", name)
	return 0
}
