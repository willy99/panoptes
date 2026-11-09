package correlation

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"
	"uuid"

	"panoptes/internal/event"
)

// The brief's defaults, so the timelines read like real ones.
var settings = Settings{QuietWindow: time.Minute, MaxDuration: 10 * time.Minute}

// clock parses a device time written as hh:mm:ss or hh:mm:ss.mmm on 2026-08-04.
func clock(t *testing.T, s string) time.Time {
	t.Helper()
	at, err := time.Parse("2006-01-02 15:04:05.999", "2026-08-04 "+s)
	if err != nil {
		t.Fatalf("bad clock %q: %v", s, err)
	}
	return at
}

// timeline parses events written as "10:00:00 10:00:30/critical ...". An
// event's id is its clock text, and its severity is info unless given.
func timeline(t *testing.T, s string, episode uuid.UUID) []Event {
	t.Helper()
	var events []Event
	for _, field := range strings.Fields(s) {
		text, sev, _ := strings.Cut(field, "/")
		e := Event{ID: text, OccurredAt: clock(t, text), Severity: event.Info, EpisodeID: episode}
		if sev != "" {
			e.Severity = map[string]event.Severity{"notice": event.Notice, "warning": event.Warning, "critical": event.Critical}[sev]
		}
		events = append(events, e)
	}
	return events
}

// every returns the clocks from start to end, step apart, as one string.
func every(t *testing.T, start, end string, step time.Duration) string {
	t.Helper()
	var out []string
	for at := clock(t, start); !at.After(clock(t, end)); at = at.Add(step) {
		out = append(out, at.Format("15:04:05"))
	}
	return strings.Join(out, " ")
}

type wantEpisode struct {
	id      string // an old episode's name, or new1, new2... in the order Rebuild asked for them
	events  string // its events' clocks, in order
	pending CloseReason
	peak    event.Severity // checked when set
}

func TestRebuild(t *testing.T) {
	tests := []struct {
		name    string
		open    map[string]string // old open episodes by name, with their events
		settled []string          // settled episodes as "first-last"
		add     string            // the new event
		want    []wantEpisode
		gone    []string
	}{
		{
			name: "a gap equal to the quiet window stays in the episode",
			open: map[string]string{"A": "10:00:00"},
			add:  "10:01:00",
			want: []wantEpisode{{id: "A", events: "10:00:00 10:01:00"}},
		},
		{
			name: "a gap one millisecond longer starts a new episode",
			open: map[string]string{"A": "10:00:00"},
			add:  "10:01:00.001",
			want: []wantEpisode{
				{id: "A", events: "10:00:00", pending: Quiet},
				{id: "new1", events: "10:01:00.001"},
			},
		},
		{
			name: "an event on the chapter boundary starts the next chapter",
			open: map[string]string{"A": every(t, "10:00:00", "10:09:00", time.Minute)},
			add:  "10:10:00",
			want: []wantEpisode{
				{id: "A", events: every(t, "10:00:00", "10:09:00", time.Minute), pending: MaxDuration},
				{id: "new1", events: "10:10:00"},
			},
		},
		{
			name: "an event one millisecond before the boundary stays",
			open: map[string]string{"A": every(t, "10:00:00", "10:09:00", time.Minute)},
			add:  "10:09:59.999",
			want: []wantEpisode{{id: "A", events: every(t, "10:00:00", "10:09:00", time.Minute) + " 10:09:59.999"}},
		},
		{
			name: "an event past the boundary and the quiet window ends the episode as quiet",
			open: map[string]string{"A": every(t, "10:00:00", "10:09:00", time.Minute)},
			add:  "10:10:30",
			want: []wantEpisode{
				{id: "A", events: every(t, "10:00:00", "10:09:00", time.Minute), pending: Quiet},
				{id: "new1", events: "10:10:30"},
			},
		},
		{
			name: "an early event moves first seen back and splits the episode into chapters",
			open: map[string]string{"A": every(t, "10:00:30", "10:09:30", time.Minute) + " 10:10:20"},
			add:  "10:00:00",
			want: []wantEpisode{
				{id: "A", events: "10:00:00 " + every(t, "10:00:30", "10:09:30", time.Minute), pending: MaxDuration},
				{id: "new1", events: "10:10:20"},
			},
		},
		{
			name: "a bridging event merges two episodes and the earlier id survives",
			open: map[string]string{"A": "10:00:00", "B": "10:02:00"},
			add:  "10:01:00",
			want: []wantEpisode{{id: "A", events: "10:00:00 10:01:00 10:02:00"}},
			gone: []string{"B"},
		},
		{
			name: "peak severity is the highest rank",
			open: map[string]string{"A": "10:00:00/warning 10:00:20/notice"},
			add:  "10:00:10/critical",
			want: []wantEpisode{{id: "A", events: "10:00:00 10:00:10 10:00:20", peak: event.Critical}},
		},
		{
			name:    "a settled episode in between keeps two open events apart",
			open:    map[string]string{"A": "10:01:50"},
			settled: []string{"10:02:00-10:02:10"},
			add:     "10:02:40",
			want: []wantEpisode{
				{id: "A", events: "10:01:50", pending: Quiet},
				{id: "new1", events: "10:02:40"},
			},
		},
		{
			name:    "a settled next chapter ends the open one at the boundary",
			open:    map[string]string{"A": every(t, "10:00:00", "10:09:00", time.Minute)},
			settled: []string{"10:10:00-10:12:00"},
			add:     "10:09:30",
			want: []wantEpisode{
				{id: "A", events: every(t, "10:00:00", "10:09:00", time.Minute) + " 10:09:30", pending: MaxDuration},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			names := map[uuid.UUID]string{}
			var events []Event
			for name, s := range tc.open {
				id := uuid.NewV4()
				names[id] = name
				events = append(events, timeline(t, s, id)...)
			}
			events = append(events, timeline(t, tc.add, uuid.UUID{})...)
			var settled []Span
			for _, s := range tc.settled {
				first, last, _ := strings.Cut(s, "-")
				settled = append(settled, Span{FirstSeen: clock(t, first), LastSeen: clock(t, last)})
			}
			newID := func() uuid.UUID {
				id := uuid.NewV4()
				names[id] = fmt.Sprintf("new%d", len(names)-len(tc.open)+1)
				return id
			}

			episodes, gone := Rebuild(settings, events, settled, newID)

			if len(episodes) != len(tc.want) {
				t.Fatalf("got %d episodes, want %d: %s", len(episodes), len(tc.want), describe(episodes, names))
			}
			for i, w := range tc.want {
				ep := episodes[i]
				var clocks []string
				for _, e := range ep.Events {
					clocks = append(clocks, e.ID)
				}
				if names[ep.ID] != w.id || strings.Join(clocks, " ") != w.events || ep.Pending != w.pending {
					t.Errorf("episode %d = %s, want %s [%s] pending %q", i, describe(episodes[i:i+1], names), w.id, w.events, w.pending)
				}
				if ep.FirstSeen != ep.Events[0].OccurredAt || ep.LastSeen != ep.Events[len(ep.Events)-1].OccurredAt {
					t.Errorf("episode %d: first seen %v, last seen %v do not match its events", i, ep.FirstSeen, ep.LastSeen)
				}
				if w.peak != 0 && ep.PeakSeverity != w.peak {
					t.Errorf("episode %d: peak severity %v, want %v", i, ep.PeakSeverity, w.peak)
				}
			}
			var goneNames []string
			for _, id := range gone {
				goneNames = append(goneNames, names[id])
			}
			if !slices.Equal(goneNames, tc.gone) {
				t.Errorf("gone = %v, want %v", goneNames, tc.gone)
			}
		})
	}
}

func describe(episodes []Episode, names map[uuid.UUID]string) string {
	var parts []string
	for _, ep := range episodes {
		var clocks []string
		for _, e := range ep.Events {
			clocks = append(clocks, e.ID)
		}
		parts = append(parts, fmt.Sprintf("%s [%s] pending %q", names[ep.ID], strings.Join(clocks, " "), ep.Pending))
	}
	return strings.Join(parts, "; ")
}

// TestRebuildIgnoresArrivalOrder feeds random timelines one event at a time,
// in random arrival orders, the way the ingest transaction does. The result
// is checked against the rules themselves, not against another rebuild:
// every event lands in exactly one episode, the episodes cover the sorted
// events in order, every episode obeys the quiet window and the max duration,
// and every cut between two episodes has a reason. Those properties pin the
// walk down to a single answer.
func TestRebuildIgnoresArrivalOrder(t *testing.T) {
	s := Settings{QuietWindow: time.Second, MaxDuration: 5 * time.Second}
	rng := rand.New(rand.NewPCG(1, 2))
	base := time.Date(2026, 8, 4, 10, 0, 0, 0, time.UTC)
	// Gaps that sit on and next to the rules' edges, and some free ones.
	edges := []time.Duration{0, time.Millisecond, 999 * time.Millisecond, time.Second, time.Second + time.Millisecond}

	for trial := range 500 {
		n := 1 + rng.IntN(25)
		var events []Event
		at := base
		for i := range n {
			if rng.IntN(2) == 0 {
				at = at.Add(edges[rng.IntN(len(edges))])
			} else {
				at = at.Add(time.Duration(rng.IntN(1500)) * time.Millisecond)
			}
			events = append(events, Event{ID: fmt.Sprintf("e%02d", i), OccurredAt: at, Severity: event.Severity(1 + rng.IntN(4))})
		}

		var episodes []Episode
		for _, i := range rng.Perm(n) {
			var stored []Event
			for _, ep := range episodes {
				for _, e := range ep.Events {
					e.EpisodeID = ep.ID
					stored = append(stored, e)
				}
			}
			episodes, _ = Rebuild(s, append(stored, events[i]), nil, uuid.NewV4)
		}

		var flat []Event
		for i, ep := range episodes {
			flat = append(flat, ep.Events...)
			for j := 1; j < len(ep.Events); j++ {
				if gap := ep.Events[j].OccurredAt.Sub(ep.Events[j-1].OccurredAt); gap > s.QuietWindow {
					t.Fatalf("trial %d: episode %d has a gap of %v", trial, i, gap)
				}
			}
			if span := ep.LastSeen.Sub(ep.FirstSeen); span >= s.MaxDuration {
				t.Fatalf("trial %d: episode %d spans %v", trial, i, span)
			}
			if i > 0 {
				prev := episodes[i-1]
				gap := ep.FirstSeen.Sub(prev.LastSeen)
				if gap <= s.QuietWindow && ep.FirstSeen.Before(prev.FirstSeen.Add(s.MaxDuration)) {
					t.Fatalf("trial %d: episodes %d and %d were cut without a reason", trial, i-1, i)
				}
			}
		}
		sorted := slices.Clone(events)
		slices.SortFunc(sorted, byDeviceTime)
		if !slices.EqualFunc(flat, sorted, func(a, b Event) bool { return a.ID == b.ID }) {
			t.Fatalf("trial %d: the episodes do not hold every event exactly once, in device-time order", trial)
		}
	}
}

func TestPlace(t *testing.T) {
	// neighbour parses "first-last" and marks the episode settled or open.
	neighbour := func(span string, settled bool) *Neighbour {
		first, last, _ := strings.Cut(span, "-")
		return &Neighbour{ID: uuid.NewV4(), FirstSeen: clock(t, first), LastSeen: clock(t, last), Settled: settled}
	}
	tests := []struct {
		name          string
		before, after *Neighbour
		at            string
		want          string // before, after or none
	}{
		{
			name:   "into the settled episode before, within one quiet window of its last seen",
			before: neighbour("10:00:00-10:05:00", true),
			after:  neighbour("10:20:00-10:21:00", true),
			at:     "10:06:00",
			want:   "before",
		},
		{
			name:   "not into the episode before, one millisecond past its quiet window",
			before: neighbour("10:00:00-10:05:00", true),
			at:     "10:06:00.001",
			want:   "none",
		},
		{
			name:   "into the settled episode after, within one quiet window of its first seen",
			before: neighbour("10:00:00-10:01:00", true),
			after:  neighbour("10:10:00-10:12:00", true),
			at:     "10:09:00",
			want:   "after",
		},
		{
			name:   "into neither, in the gap between two settled episodes",
			before: neighbour("10:00:00-10:01:00", true),
			after:  neighbour("10:10:00-10:11:00", true),
			at:     "10:05:00",
			want:   "none",
		},
		{
			name:  "not into the episode after when joining it would reach the max duration",
			after: neighbour("10:10:00-10:19:50", true),
			at:    "10:09:50",
			want:  "none",
		},
		{
			name:  "into the episode after when joining it stays one millisecond short of the max duration",
			after: neighbour("10:10:00-10:19:50", true),
			at:    "10:09:50.001",
			want:  "after",
		},
		{
			// ADR-0005: a chapter cut at 10:10:00, then a late event at
			// 10:09:45. It is within reach of both, and the earlier chapter
			// takes it, so the next chapter's first seen does not move back
			// across the boundary.
			name:   "the 10:09:45 event joins the chapter that started at 10:00:00",
			before: neighbour("10:00:00-10:09:30", true),
			after:  neighbour("10:10:00-10:12:00", false),
			at:     "10:09:45",
			want:   "before",
		},
		{
			name:   "not into the episode before at its chapter boundary",
			before: neighbour("10:00:00-10:09:50", true),
			at:     "10:10:00",
			want:   "none",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := "none"
			switch Place(settings, clock(t, tc.at), tc.before, tc.after) {
			case nil:
			case tc.before:
				got = "before"
			case tc.after:
				got = "after"
			}
			if got != tc.want {
				t.Errorf("Place = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestCorrectRecomputesTheAggregates(t *testing.T) {
	id := uuid.NewV4()
	ep := Correct(id, timeline(t, "10:00:10/warning 10:00:20", id), timeline(t, "10:00:00/critical", uuid.UUID{})[0])
	if ep.ID != id || ep.FirstSeen != clock(t, "10:00:00") || ep.LastSeen != clock(t, "10:00:20") ||
		len(ep.Events) != 3 || ep.PeakSeverity != event.Critical {
		t.Errorf("corrected episode: %s, first %v, last %v, %d events, peak %v; want the same id, 10:00:00, 10:00:20, 3, critical",
			ep.ID, ep.FirstSeen, ep.LastSeen, len(ep.Events), ep.PeakSeverity)
	}
}
