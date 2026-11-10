package e2e

import (
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"
)

// These tests run with the harness's test settings: a quiet window of 1s, a
// max duration of 5s and a settle grace of 200ms.

// waitSettled waits until the episode with the given id has settled and
// returns it.
func (s *scenario) waitSettled(p *proc, id string) episodeJSON {
	s.t.Helper()
	var ep episodeJSON
	waitFor(s.t, 5*time.Second, "episode "+id+" settling", func() bool {
		ep, _ = s.episode(p, id)
		return ep.Status == "settled"
	})
	return ep
}

// summary renders announcements as "settled" or "corrected:<revision>".
func summary(got []announcement) []string {
	var out []string
	for _, a := range got {
		if rev, ok := a.Fields["revision"]; ok {
			out = append(out, "corrected:"+string(rev))
		} else {
			out = append(out, "settled")
		}
	}
	return out
}

func TestALateEventBecomesACorrection(t *testing.T) {
	p := startReady(t)
	s := newScenario(t)

	s.publish(s.event(1, "info", 0), s.event(2, "warning", 200*ms))
	s.waitDrained()
	id := s.episodes(p)[0].ID
	settled := s.waitSettled(p, id)

	// Within one quiet window of the settled episode's last seen.
	s.publish(s.event(3, "critical", 400*ms))
	s.waitDrained()

	corrected, _ := s.episode(p, id)
	if corrected.EventCount != 3 || corrected.LastSeen != at(400*ms) || corrected.PeakSeverity != "critical" {
		t.Errorf("after the late event: %d events, last seen %s, peak %s; want 3, %s, critical",
			corrected.EventCount, corrected.LastSeen, corrected.PeakSeverity, at(400*ms))
	}
	if corrected.Status != "settled" || *corrected.SettledAt != *settled.SettledAt || *corrected.CloseReason != *settled.CloseReason {
		t.Errorf("a correction changed the settled fields: before %+v, after %+v", settled, corrected)
	}
	if n := len(s.episodes(p)); n != 1 {
		t.Errorf("got %d episodes, want the one corrected", n)
	}

	var got []announcement
	waitFor(t, 5*time.Second, "the correction's announcement", func() bool {
		got = s.announcements()
		return len(got) >= 2
	})
	time.Sleep(500 * time.Millisecond)
	got = s.announcements()
	if want := []string{"settled", "corrected:1"}; !slices.Equal(summary(got), want) {
		t.Fatalf("announcements %v, want %v", summary(got), want)
	}
	c := got[1]
	if c.Subject != "episode.corrected."+s.site+".cam-1" || c.MsgID != "corrected:"+id+":1" {
		t.Errorf("correction on %s with id %s; want episode.corrected.%s.cam-1 and corrected:%s:1", c.Subject, c.MsgID, s.site, id)
	}
	var count int
	_ = json.Unmarshal(c.Fields["event_count"], &count)
	if count != 3 || string(c.Fields["episode_id"]) != fmt.Sprintf("%q", id) {
		t.Errorf("correction carries event_count %d and episode_id %s; want 3 and %q", count, c.Fields["episode_id"], id)
	}
}

func TestTwoLateEventsGiveRevisionsOneAndTwo(t *testing.T) {
	p := startReady(t)
	s := newScenario(t)

	s.publish(s.event(1, "info", 0), s.event(2, "info", 200*ms))
	s.waitDrained()
	id := s.episodes(p)[0].ID
	s.waitSettled(p, id)

	s.publish(s.event(3, "info", 300*ms), s.event(4, "info", 500*ms))
	s.waitDrained()

	var got []announcement
	waitFor(t, 5*time.Second, "both corrections", func() bool {
		got = s.announcements()
		return len(got) >= 3
	})
	if want := []string{"settled", "corrected:1", "corrected:2"}; !slices.Equal(summary(got), want) {
		t.Errorf("announcements %v, want %v", summary(got), want)
	}
	if ep, _ := s.episode(p, id); ep.EventCount != 4 {
		t.Errorf("event count %d, want 4", ep.EventCount)
	}
}

// The 10:09:45 timeline from ADR-0005, scaled to a 5s max duration: a chapter
// that started at 0s is cut at 5s, and a late event at 4.75s arrives after it
// settled. It joins that chapter, and the next chapter keeps its first seen.
func TestALateEventBeforeAChapterBoundaryJoinsTheEarlierChapter(t *testing.T) {
	p := startReady(t)
	s := newScenario(t)

	for i := range 12 { // 0s to 5.5s, half a second apart: two chapters
		s.publish(s.event(i, "info", time.Duration(i)*500*ms))
	}
	s.waitDrained()
	episodes := s.episodes(p)
	if len(episodes) != 2 {
		t.Fatalf("got %d episodes, want two chapters: %+v", len(episodes), episodes)
	}
	first, second := episodes[1], episodes[0]
	s.waitSettled(p, first.ID)

	s.publish(s.event(99, "info", 4750*ms))
	s.waitDrained()

	firstAfter, _ := s.episode(p, first.ID)
	secondAfter, _ := s.episode(p, second.ID)
	if firstAfter.EventCount != 11 || firstAfter.LastSeen != at(4750*ms) {
		t.Errorf("first chapter: %d events, last seen %s; want 11 and %s", firstAfter.EventCount, firstAfter.LastSeen, at(4750*ms))
	}
	if secondAfter.FirstSeen != at(5*time.Second) || secondAfter.EventCount != 2 {
		t.Errorf("second chapter: first seen %s, %d events; want %s and 2", secondAfter.FirstSeen, secondAfter.EventCount, at(5*time.Second))
	}
}

func TestALateEventThatReachesNoEpisodeStartsItsOwn(t *testing.T) {
	p := startReady(t)
	s := newScenario(t)

	s.publish(s.event(1, "info", 0), s.event(2, "info", 200*ms), s.event(3, "info", 4*time.Second), s.event(4, "info", 4200*ms))
	s.waitDrained()
	for _, e := range s.episodes(p) {
		s.waitSettled(p, e.ID)
	}

	// Two seconds from both: beyond the quiet window of either.
	s.publish(s.event(5, "warning", 2*time.Second))
	s.waitDrained()
	episodes := s.episodes(p)
	if len(episodes) != 3 || episodes[1].FirstSeen != at(2*time.Second) || episodes[1].EventCount != 1 {
		t.Fatalf("episodes %+v, want a third one at 2s with one event", episodes)
	}
	s.waitSettled(p, episodes[1].ID)

	var got []announcement
	waitFor(t, 5*time.Second, "three announcements", func() bool {
		got = s.announcements()
		return len(got) >= 3
	})
	if want := []string{"settled", "settled", "settled"}; !slices.Equal(summary(got), want) {
		t.Errorf("announcements %v, want %v", summary(got), want)
	}
}

// A correction can move a settled episode's first seen back, and that can
// change why the open episode before it ends. Here the open episode P runs
// from 0s to 4.3s, so its chapter boundary is 5s, and the settled episode N
// starts at 6s: more than a quiet window after P's last event, so P has ended
// as quiet. A late event at 5.05s is past P's boundary and within a quiet
// window of N, so it corrects N, whose first seen moves to 5.05s. Now the
// episode after P starts at P's boundary and within a quiet window of its
// last event: P must settle as max_duration.
func TestACorrectionThatMovesTheNextEpisodeBackChangesWhyTheOpenOneEnds(t *testing.T) {
	// A long grace keeps P open until the late event has been read.
	p := startReady(t, "EPISODE_SETTLE_GRACE=2s")
	s := newScenario(t)

	s.publish(s.event(1, "info", 6*time.Second), s.event(2, "info", 6500*ms))
	s.waitDrained()
	n := s.episodes(p)[0]
	s.waitSettled(p, n.ID)

	for i := range 9 { // 0s to 4s
		s.publish(s.event(10+i, "info", time.Duration(i)*500*ms))
	}
	s.publish(s.event(19, "info", 4300*ms))
	s.waitDrained()
	s.publish(s.event(20, "info", 5050*ms))
	s.waitDrained()

	nAfter, _ := s.episode(p, n.ID)
	if nAfter.FirstSeen != at(5050*ms) || nAfter.EventCount != 3 {
		t.Fatalf("N after the late event: first seen %s, %d events; want %s and 3", nAfter.FirstSeen, nAfter.EventCount, at(5050*ms))
	}
	var pID string
	for _, e := range s.episodes(p) {
		if e.FirstSeen == at(0) {
			pID = e.ID
		}
	}
	if settled := s.waitSettled(p, pID); *settled.CloseReason != "max_duration" || settled.EventCount != 10 {
		t.Errorf("P settled as %s with %d events, want max_duration with 10", *settled.CloseReason, settled.EventCount)
	}
}
