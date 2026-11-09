package e2e

import (
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"
	"uuid"
)

// Device times in these tests stay within a few hundred milliseconds of each
// other, well inside the test quiet window and max duration, so every key
// forms exactly one episode under any grouping rule.

func TestEventsOfOneKeyFormOneEpisode(t *testing.T) {
	p := startReady(t)
	s := newScenario(t)

	// Ids run against device time, and two events share a device time, so
	// only an order by device time, then event_id, gives the expected list.
	first := s.event(3, "info", 0)
	first.Attributes = json.RawMessage(`{"track_id":4412,"confidence":0.87,"box":[1,2,3,4],"zone":{"name":"gate","id":7}}`)
	second := s.event(1, "warning", 300*ms) // no attributes
	tied := s.event(4, "info", 300*ms)
	last := s.event(2, "notice", 600*ms)
	last.Attributes = json.RawMessage(`{"note":"<b>gate</b>"}`)
	// Out of device-time order, so the order of the results is the service's doing.
	s.publish(last, tied, first, second)
	s.waitDrained()

	episodes := s.episodes(p)
	if len(episodes) != 1 {
		t.Fatalf("got %d episodes, want 1: %+v", len(episodes), episodes)
	}
	got := episodes[0]
	if got.FirstSeen != at(0) || got.LastSeen != at(600*ms) || got.EventCount != 4 || got.PeakSeverity != "warning" {
		t.Errorf("first seen %s, last seen %s, event count %d, peak severity %s; want %s, %s, 4, warning",
			got.FirstSeen, got.LastSeen, got.EventCount, got.PeakSeverity, at(0), at(600*ms))
	}

	episode, events := s.episode(p, got.ID)
	if episode.ID != got.ID || episode.EventCount != got.EventCount || episode.LastSeen != got.LastSeen {
		t.Errorf("GET /episodes/{id} episode %+v differs from the list's %+v", episode, got)
	}
	// Each event exactly as published, attributes byte for byte: key order
	// kept, "<" not escaped, and {} where the device sent none.
	want := []eventJSON{
		{EventID: first.ID, Severity: "info", OccurredAt: at(0), Attributes: first.Attributes},
		{EventID: second.ID, Severity: "warning", OccurredAt: at(300 * ms), Attributes: json.RawMessage(`{}`)},
		{EventID: tied.ID, Severity: "info", OccurredAt: at(300 * ms), Attributes: json.RawMessage(`{}`)},
		{EventID: last.ID, Severity: "notice", OccurredAt: at(600 * ms), Attributes: last.Attributes},
	}
	if !slices.EqualFunc(events, want, sameEvent) {
		t.Errorf("events:\n got  %s\n want %s", show(events), show(want))
	}
}

func TestDuplicatesAreStoredOnceAndTheFirstCopyWins(t *testing.T) {
	p := startReady(t)
	s := newScenario(t)

	first := s.event(1, "info", 0)
	second := s.event(2, "warning", 200*ms)
	second.Attributes = json.RawMessage(`{"copy":"original"}`)
	third := s.event(3, "notice", 400*ms)

	// Two copies of each, back to back, so that workers race on one event_id.
	s.publish(first, first, second, second, third, third)
	s.waitDrained()

	// A third copy of each, once the first is stored. The copy of the second
	// event differs in everything but its event_id, down to the camera, which
	// puts it under another episode key and another lock.
	altered := second
	altered.Camera = "cam-2"
	altered.Severity = "critical"
	altered.OccurredAt = at(900 * ms)
	altered.Attributes = json.RawMessage(`{"copy":"altered"}`)
	s.publish(first, altered, third)
	s.waitDrained()

	episodes := s.episodes(p)
	if len(episodes) != 1 {
		t.Fatalf("got %d episodes, want 1 (the altered copy must not start one at cam-2): %+v", len(episodes), episodes)
	}
	got := episodes[0]
	if got.CameraID != "cam-1" || got.EventCount != 3 || got.LastSeen != at(400*ms) || got.PeakSeverity != "warning" {
		t.Errorf("camera %s, event count %d, last seen %s, peak severity %s; want cam-1, 3, %s, warning",
			got.CameraID, got.EventCount, got.LastSeen, got.PeakSeverity, at(400*ms))
	}
	_, events := s.episode(p, got.ID)
	want := []eventJSON{
		{EventID: first.ID, Severity: "info", OccurredAt: at(0), Attributes: json.RawMessage(`{}`)},
		{EventID: second.ID, Severity: "warning", OccurredAt: at(200 * ms), Attributes: second.Attributes},
		{EventID: third.ID, Severity: "notice", OccurredAt: at(400 * ms), Attributes: json.RawMessage(`{}`)},
	}
	if !slices.EqualFunc(events, want, sameEvent) {
		t.Errorf("events:\n got  %s\n want %s", show(events), show(want))
	}
}

func TestOneEventIDUnderTwoKeysAtOnceIsStoredOnce(t *testing.T) {
	p := startReady(t)
	s := newScenario(t)

	// Each event goes out twice back to back, at two cameras. The copies take
	// different key locks, so workers run them at the same time and meet only
	// on the event_id. Ten pairs make at least one real collision likely.
	const pairs = 10
	for n := range pairs {
		e := s.event(n, "info", time.Duration(n)*100*ms)
		other := e
		other.Camera = "cam-2"
		s.publish(e, other)
	}
	s.waitDrained()

	seen := map[string]int{}
	for _, e := range s.episodes(p) {
		_, events := s.episode(p, e.ID)
		for _, ev := range events {
			seen[ev.EventID]++
		}
	}
	if len(seen) != pairs {
		t.Errorf("stored %d distinct events, want %d", len(seen), pairs)
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("event %s is in %d episodes, want 1", id, n)
		}
	}
}

func TestKindsAtOneCameraFormSeparateEpisodes(t *testing.T) {
	p := startReady(t)
	s := newScenario(t)

	person := s.event(1, "warning", 0)
	tamper := s.event(2, "critical", 0)
	tamper.Kind = "tamper"
	s.publish(person, tamper)
	s.waitDrained()

	episodes := s.episodes(p)
	if len(episodes) != 2 {
		t.Fatalf("got %d episodes, want 2: %+v", len(episodes), episodes)
	}
	byKind := map[string]episodeJSON{}
	for _, e := range episodes {
		byKind[e.Kind] = e
	}
	for kind, severity := range map[string]string{"person_detected": "warning", "tamper": "critical"} {
		if e := byKind[kind]; e.EventCount != 1 || e.PeakSeverity != severity {
			t.Errorf("%s: event count %d, peak severity %q; want 1, %s", kind, e.EventCount, e.PeakSeverity, severity)
		}
	}
}

func TestPeakSeverityIsTheHighestRank(t *testing.T) {
	p := startReady(t)
	s := newScenario(t)

	// As strings, "warning" sorts after "critical".
	s.publish(s.event(1, "warning", 0), s.event(2, "critical", 100*ms), s.event(3, "warning", 200*ms))
	s.waitDrained()

	episodes := s.episodes(p)
	if len(episodes) != 1 || episodes[0].PeakSeverity != "critical" {
		t.Fatalf("episodes %+v; want one with peak severity critical", episodes)
	}
}

func TestAnOpenEpisodeHasTheContractShape(t *testing.T) {
	// A long quiet window keeps the episode open while the test reads it.
	p := startReady(t, "EPISODE_QUIET_WINDOW=1h")
	s := newScenario(t)

	precise := s.event(1, "info", 0)
	precise.OccurredAt = "2026-08-04T10:00:00.123456Z"
	s.publish(precise, s.event(2, "info", 500*ms))
	s.waitDrained()

	var list map[string]json.RawMessage
	p.getJSON("/episodes?site_id="+s.site, http.StatusOK, &list)
	assertKeys(t, "list", list, "episodes", "next_cursor")
	if string(list["next_cursor"]) != `""` {
		t.Errorf(`next_cursor = %s, want ""`, list["next_cursor"])
	}
	var items []json.RawMessage
	if err := json.Unmarshal(list["episodes"], &items); err != nil || len(items) != 1 {
		t.Fatalf("episodes = %s, want one episode", list["episodes"])
	}
	var item map[string]json.RawMessage
	var e episodeJSON
	if json.Unmarshal(items[0], &item) != nil || json.Unmarshal(items[0], &e) != nil {
		t.Fatalf("episode %s is not an episode object", items[0])
	}
	assertKeys(t, "episode", item, "id", "site_id", "camera_id", "kind", "status", "first_seen", "last_seen",
		"event_count", "peak_severity", "close_reason", "settled_at")
	if _, err := uuid.Parse(e.ID); err != nil {
		t.Errorf("id %q is not a UUID", e.ID)
	}
	if e.Status != "open" || string(item["close_reason"]) != "null" || string(item["settled_at"]) != "null" {
		t.Errorf("status %q, close_reason %s, settled_at %s; want open, null, null",
			e.Status, item["close_reason"], item["settled_at"])
	}
	if e.FirstSeen != "2026-08-04T10:00:00.123456Z" || e.LastSeen != at(500*ms) {
		t.Errorf("first seen %s, last seen %s; want 2026-08-04T10:00:00.123456Z, %s", e.FirstSeen, e.LastSeen, at(500*ms))
	}

	var detail map[string]json.RawMessage
	p.getJSON("/episodes/"+e.ID, http.StatusOK, &detail)
	assertKeys(t, "detail", detail, "episode", "events")
	if string(detail["episode"]) != string(items[0]) {
		t.Errorf("detail episode %s differs from the list's %s", detail["episode"], items[0])
	}
	var events []map[string]json.RawMessage
	if err := json.Unmarshal(detail["events"], &events); err != nil || len(events) != 2 {
		t.Fatalf("events = %s, want two events", detail["events"])
	}
	for _, ev := range events {
		assertKeys(t, "event", ev, "event_id", "severity", "occurred_at", "attributes")
	}
}

func TestUnknownAndMalformedEpisodeIDsAreNotFound(t *testing.T) {
	p := startReady(t)
	for _, id := range []string{uuid.NewV4().String(), "not-a-uuid", "", "a/b"} {
		var body struct {
			Error string `json:"error"`
		}
		p.getJSON("/episodes/"+id, http.StatusNotFound, &body)
		if body.Error == "" {
			t.Errorf("GET /episodes/%s: no error message", id)
		}
	}
}

// assertKeys checks that a JSON object has exactly the given keys.
func assertKeys(t *testing.T, what string, object map[string]json.RawMessage, keys ...string) {
	t.Helper()
	var got []string
	for k := range object {
		got = append(got, k)
	}
	slices.Sort(got)
	slices.Sort(keys)
	if !slices.Equal(got, keys) {
		t.Errorf("%s keys = %v, want %v", what, got, keys)
	}
}

// sameEvent compares two events, attributes byte for byte.
func sameEvent(a, b eventJSON) bool {
	return a.EventID == b.EventID && a.Severity == b.Severity && a.OccurredAt == b.OccurredAt &&
		string(a.Attributes) == string(b.Attributes)
}

// show renders events for a failure message, attributes as they came.
func show(events []eventJSON) string {
	var b strings.Builder
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(events)
	return strings.TrimSpace(b.String())
}
