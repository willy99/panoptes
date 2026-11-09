// Package correlation holds the rules that group one episode key's events
// into episodes by device time. It does no IO: the store loads the key's
// state, calls Place and Rebuild inside the ingest transaction and writes the
// result.
package correlation

import (
	"slices"
	"strings"
	"time"
	"uuid"

	"panoptes/internal/event"
)

// Settings are the two windows the rules use.
type Settings struct {
	QuietWindow time.Duration // longest device-time gap inside one episode
	MaxDuration time.Duration // an episode spans [first seen, first seen + MaxDuration)
}

// CloseReason is why an episode settles, or will settle.
type CloseReason string

const (
	Quiet       CloseReason = "quiet"
	MaxDuration CloseReason = "max_duration"
)

// Event is a stored event as the rules see it.
type Event struct {
	ID         string
	OccurredAt time.Time // device time
	Severity   event.Severity
	ArrivedAt  time.Time
	EpisodeID  uuid.UUID // the open episode that holds it; zero for the event being added
}

// Span is a settled episode as the rules see it: where it lies in device time.
type Span struct {
	FirstSeen time.Time
	LastSeen  time.Time
}

// Episode is an open episode as a rebuild produces it.
type Episode struct {
	ID           uuid.UUID
	Events       []Event // in device-time order, ties by event_id
	FirstSeen    time.Time
	LastSeen     time.Time
	PeakSeverity event.Severity
	LastArrival  time.Time
	// Pending is set when another episode of the key follows this one, so it
	// has ended by device time and only waits to settle: MaxDuration when the
	// next chapter started at its boundary, Quiet otherwise.
	Pending CloseReason
}

// Neighbour is an episode of the key next to a new event in device time.
type Neighbour struct {
	ID        uuid.UUID
	FirstSeen time.Time
	LastSeen  time.Time
	Settled   bool
}

// Place returns the episode a new event at device time at belongs to, by the
// placement rule (ADR-0005). before is the key's episode with the greatest
// first seen at or before the event, after the one with the smallest first
// seen after it; either may be nil. The event goes to before when it is
// within reach of it: no more than one quiet window after its last seen, and
// before its chapter boundary. Otherwise it goes to after when it is within
// reach of that one: no more than one quiet window before its first seen, and
// keeping its span shorter than the max duration. Otherwise Place returns nil.
//
// Only a settled result matters to the caller: the event then joins that
// episode as a correction. An open result, or nil, means the key's open
// episodes are rebuilt, which puts the event where the walk says.
func Place(s Settings, at time.Time, before, after *Neighbour) *Neighbour {
	if before != nil && at.Sub(before.LastSeen) <= s.QuietWindow && at.Before(before.FirstSeen.Add(s.MaxDuration)) {
		return before
	}
	if after != nil && after.FirstSeen.Sub(at) <= s.QuietWindow && after.LastSeen.Sub(at) < s.MaxDuration {
		return after
	}
	return nil
}

// Correct adds a late event to a settled episode's events and recomputes its
// aggregates. The episode keeps its id; its settled fields are the caller's.
func Correct(id uuid.UUID, events []Event, late Event) Episode {
	all := append(slices.Clone(events), late)
	slices.SortFunc(all, byDeviceTime)
	return aggregate(id, all)
}

// Rebuild groups the events of a key's open episodes, plus the event being
// added, into open episodes. It walks the events in device-time order: an
// event joins the current episode when its gap to the episode's last seen is
// at most the quiet window and it lies before first seen plus the max
// duration; otherwise it starts the next episode. Settled episodes are fixed
// dividers that no open episode spans, and they count as the next episode
// when deciding whether an open one has ended.
//
// Each resulting episode keeps the id of the old open episode that held its
// earliest previously stored event, unless an earlier episode already took
// that id; otherwise it gets newID(). Rebuild also returns the old ids that
// no episode kept, which the caller deletes.
//
// The result depends only on the set of events, never on the order in which
// they arrived.
func Rebuild(s Settings, events []Event, settled []Span, newID func() uuid.UUID) (episodes []Episode, gone []uuid.UUID) {
	sorted := slices.Clone(events)
	slices.SortFunc(sorted, byDeviceTime)

	var groups [][]Event
	for _, e := range sorted {
		if n := len(groups); n > 0 {
			g := groups[n-1]
			first, last := g[0].OccurredAt, g[len(g)-1].OccurredAt
			if e.OccurredAt.Sub(last) <= s.QuietWindow &&
				e.OccurredAt.Before(first.Add(s.MaxDuration)) &&
				!divided(settled, last, e.OccurredAt) {
				groups[n-1] = append(g, e)
				continue
			}
		}
		groups = append(groups, []Event{e})
	}

	kept := map[uuid.UUID]bool{}
	for _, g := range groups {
		id := previousEpisode(g)
		if id == (uuid.UUID{}) || kept[id] {
			id = newID()
		}
		kept[id] = true
		episodes = append(episodes, aggregate(id, g))
	}

	for i := range episodes {
		episodes[i].Pending = pending(s, episodes, settled, i)
	}

	for _, e := range sorted {
		if id := e.EpisodeID; id != (uuid.UUID{}) && !kept[id] && !slices.Contains(gone, id) {
			gone = append(gone, id)
		}
	}
	return episodes, gone
}

// byDeviceTime orders events by device time, ties by event_id byte by byte,
// the same order the events table uses.
func byDeviceTime(a, b Event) int {
	if c := a.OccurredAt.Compare(b.OccurredAt); c != 0 {
		return c
	}
	return strings.Compare(a.ID, b.ID)
}

// divided reports whether a settled episode lies between device times a and b,
// so that an open episode containing both would span it.
func divided(settled []Span, a, b time.Time) bool {
	for _, sp := range settled {
		if (a.Before(sp.FirstSeen) && !b.Before(sp.FirstSeen)) || (!sp.LastSeen.Before(a) && sp.LastSeen.Before(b)) {
			return true
		}
	}
	return false
}

// previousEpisode returns the episode that held the group's earliest
// previously stored event, or zero when the group holds only the new event.
func previousEpisode(g []Event) uuid.UUID {
	for _, e := range g {
		if e.EpisodeID != (uuid.UUID{}) {
			return e.EpisodeID
		}
	}
	return uuid.UUID{}
}

// aggregate builds an episode from its events, which are in device-time order.
func aggregate(id uuid.UUID, events []Event) Episode {
	ep := Episode{
		ID:        id,
		Events:    events,
		FirstSeen: events[0].OccurredAt,
		LastSeen:  events[len(events)-1].OccurredAt,
	}
	for _, e := range events {
		ep.PeakSeverity = max(ep.PeakSeverity, e.Severity)
		if e.ArrivedAt.After(ep.LastArrival) {
			ep.LastArrival = e.ArrivedAt
		}
	}
	return ep
}

// pending decides whether episode i has ended by device time, and why.
func pending(s Settings, episodes []Episode, settled []Span, i int) CloseReason {
	ep := episodes[i]
	var starts []time.Time // first seen of every episode that starts after this one
	if i+1 < len(episodes) {
		starts = append(starts, episodes[i+1].FirstSeen)
	}
	for _, sp := range settled {
		if sp.FirstSeen.After(ep.FirstSeen) {
			starts = append(starts, sp.FirstSeen)
		}
	}
	if len(starts) == 0 {
		return ""
	}
	next := slices.MinFunc(starts, time.Time.Compare)
	if !next.Before(ep.FirstSeen.Add(s.MaxDuration)) && next.Sub(ep.LastSeen) <= s.QuietWindow {
		return MaxDuration
	}
	return Quiet
}
