package e2e

import (
	"math/rand/v2"
	"net/http"
	"testing"
	"time"
)

// A timeline under openSettings (quiet window 1m, max duration 5m) that
// touches both edges of both rules:
//   - 0s to 120s: the last gap is exactly the quiet window, so it stays;
//   - 180.001s: a gap one millisecond longer starts the second episode,
//     whose chapter boundary is then 480.001s;
//   - 480s: one millisecond before the boundary, so it stays;
//   - 480.001s: exactly on the boundary, so it starts the third chapter.
var chapterTimeline = []time.Duration{
	0, 30 * time.Second, 60 * time.Second, 120 * time.Second,
	180*time.Second + ms, 240 * time.Second, 300 * time.Second, 360 * time.Second, 420 * time.Second, 480 * time.Second,
	480*time.Second + ms, 510 * time.Second,
}

// chapterEpisodes is what the timeline must give, newest first as listed.
var chapterEpisodes = []struct {
	first, last time.Duration
	count       int
}{
	{480*time.Second + ms, 510 * time.Second, 2},
	{180*time.Second + ms, 480 * time.Second, 6},
	{0, 120 * time.Second, 4},
}

func TestEpisodesFollowDeviceTimeWhateverTheArrivalOrder(t *testing.T) {
	p := startReady(t, openSettings...)

	sorted, shuffled := newScenario(t), newScenario(t)
	order := rand.New(rand.NewPCG(7, 7)).Perm(len(chapterTimeline))
	for i := range chapterTimeline {
		sorted.publish(sorted.event(i, "info", chapterTimeline[i]))
		j := order[i]
		shuffled.publish(shuffled.event(j, "info", chapterTimeline[j]))
	}
	sorted.waitDrained()
	shuffled.waitDrained()

	for name, s := range map[string]*scenario{"sorted": sorted, "shuffled": shuffled} {
		episodes := s.episodes(p)
		if len(episodes) != len(chapterEpisodes) {
			t.Fatalf("%s: got %d episodes, want %d: %+v", name, len(episodes), len(chapterEpisodes), episodes)
		}
		total := 0
		for i, want := range chapterEpisodes {
			got := episodes[i]
			total += got.EventCount
			if got.FirstSeen != at(want.first) || got.LastSeen != at(want.last) || got.EventCount != want.count {
				t.Errorf("%s: episode %d spans %s to %s with %d events, want %s to %s with %d",
					name, i, got.FirstSeen, got.LastSeen, got.EventCount, at(want.first), at(want.last), want.count)
			}
		}
		if total != len(chapterTimeline) {
			t.Errorf("%s: chapters hold %d events, want %d", name, total, len(chapterTimeline))
		}
	}
}

func TestAnEpisodeMergedIntoAnotherIsGone(t *testing.T) {
	p := startReady(t, openSettings...)
	s := newScenario(t)

	// Two minutes apart, more than the quiet window: two episodes.
	s.publish(s.event(1, "info", 0), s.event(2, "info", 2*time.Minute))
	s.waitDrained()
	before := s.episodes(p)
	if len(before) != 2 {
		t.Fatalf("got %d episodes before the bridge, want 2: %+v", len(before), before)
	}
	later, earlier := before[0], before[1]

	// One minute after the first and before the second: it bridges them.
	s.publish(s.event(3, "info", time.Minute))
	s.waitDrained()
	after := s.episodes(p)
	if len(after) != 1 || after[0].EventCount != 3 {
		t.Fatalf("episodes after the bridge = %+v, want one with 3 events", after)
	}
	if after[0].ID != earlier.ID {
		t.Errorf("the merged episode has id %s, want the earlier episode's %s", after[0].ID, earlier.ID)
	}
	var body struct {
		Error string `json:"error"`
	}
	p.getJSON("/episodes/"+later.ID, http.StatusNotFound, &body)
	if body.Error == "" {
		t.Error("GET the merged-away id: no error message")
	}
}
