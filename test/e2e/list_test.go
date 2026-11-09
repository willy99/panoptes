package e2e

import (
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"
)

// listFixture publishes one event per episode, so that each episode is named
// by its camera and kind. Two of them share a first seen, to test the
// tiebreak.
func listFixture(t *testing.T) (*proc, *scenario) {
	p := startReady(t, openSettings...)
	s := newScenario(t)
	for i, e := range []struct {
		camera, kind string
		offset       time.Duration
	}{
		{"cam-1", "person_detected", 0},
		{"cam-1", "tamper", 0},
		{"cam-2", "person_detected", time.Minute},
		{"cam-2", "loitering", 2 * time.Minute},
		{"cam-3", "line_crossed", 3 * time.Minute},
	} {
		ev := s.event(i, "info", e.offset)
		ev.Camera, ev.Kind = e.camera, e.kind
		s.publish(ev)
	}
	s.waitDrained()
	return p, s
}

// list returns the episodes of one GET /episodes as "camera/kind" names.
func (s *scenario) list(p *proc, query string) []string {
	s.t.Helper()
	var body listJSON
	p.getJSON("/episodes?site_id="+s.site+query, http.StatusOK, &body)
	var names []string
	for _, e := range body.Episodes {
		names = append(names, e.CameraID+"/"+e.Kind)
	}
	return names
}

type listJSON struct {
	Episodes   []episodeJSON `json:"episodes"`
	NextCursor string        `json:"next_cursor"`
}

func TestListFilters(t *testing.T) {
	p, s := listFixture(t)
	from, to := url.QueryEscape(at(time.Minute)), url.QueryEscape(at(2*time.Minute))

	for _, c := range []struct {
		query string
		want  []string // as a set; order has its own test
	}{
		{"", []string{"cam-1/person_detected", "cam-1/tamper", "cam-2/person_detected", "cam-2/loitering", "cam-3/line_crossed"}},
		{"&camera_id=&kind=&status=&from=&to=&limit=&cursor=", []string{"cam-1/person_detected", "cam-1/tamper", "cam-2/person_detected", "cam-2/loitering", "cam-3/line_crossed"}},
		{"&camera_id=cam-2", []string{"cam-2/person_detected", "cam-2/loitering"}},
		{"&kind=person_detected", []string{"cam-1/person_detected", "cam-2/person_detected"}},
		{"&camera_id=cam-2&kind=loitering", []string{"cam-2/loitering"}},
		{"&status=open", []string{"cam-1/person_detected", "cam-1/tamper", "cam-2/person_detected", "cam-2/loitering", "cam-3/line_crossed"}},
		{"&status=settled", nil},
		{"&from=" + from, []string{"cam-2/person_detected", "cam-2/loitering", "cam-3/line_crossed"}},
		{"&to=" + to, []string{"cam-1/person_detected", "cam-1/tamper", "cam-2/person_detected"}},
		{"&from=" + from + "&to=" + to, []string{"cam-2/person_detected"}},
		{"&from=" + to + "&to=" + to, nil},
		{"&from=" + to + "&to=" + from, nil},
		{"&kind=person_detected&from=" + from, []string{"cam-2/person_detected"}},
	} {
		got := s.list(p, c.query)
		slices.Sort(got)
		slices.Sort(c.want)
		if !slices.Equal(got, c.want) {
			t.Errorf("?site_id=…%s: got %v, want %v", c.query, got, c.want)
		}
	}
}

func TestListOrderIsFirstSeenThenIDDescending(t *testing.T) {
	p, s := listFixture(t)
	var body listJSON
	p.getJSON("/episodes?site_id="+s.site, http.StatusOK, &body)
	e := body.Episodes
	if len(e) != 5 {
		t.Fatalf("got %d episodes, want 5", len(e))
	}
	for i := 1; i < len(e); i++ {
		if e[i-1].FirstSeen < e[i].FirstSeen || (e[i-1].FirstSeen == e[i].FirstSeen && e[i-1].ID < e[i].ID) {
			t.Errorf("episodes %d and %d are out of order: %s %s, then %s %s", i-1, i, e[i-1].FirstSeen, e[i-1].ID, e[i].FirstSeen, e[i].ID)
		}
	}
	if e[3].FirstSeen != e[4].FirstSeen {
		t.Errorf("the fixture's two episodes with equal first seen are not last: %+v", e)
	}
}

func TestWalkingEveryPageReturnsEachEpisodeOnce(t *testing.T) {
	p, s := listFixture(t)
	var all []string
	cursor, pages := "", 0
	for {
		var body listJSON
		p.getJSON("/episodes?site_id="+s.site+"&limit=2&cursor="+url.QueryEscape(cursor), http.StatusOK, &body)
		pages++
		if len(body.Episodes) > 2 {
			t.Fatalf("page %d has %d episodes with limit=2", pages, len(body.Episodes))
		}
		for _, e := range body.Episodes {
			all = append(all, e.ID)
		}
		if body.NextCursor == "" {
			break
		}
		if pages > 5 {
			t.Fatal("more than 5 pages for 5 episodes")
		}
		cursor = body.NextCursor
	}
	// Five episodes in pages of two: the pair with equal first seen is split
	// across the last two pages, which only a total order keeps apart.
	if pages != 3 || len(all) != 5 {
		t.Errorf("%d pages with %d episodes, want 3 pages with 5", pages, len(all))
	}
	unique := slices.Clone(all)
	slices.Sort(unique)
	if len(slices.Compact(unique)) != 5 {
		t.Errorf("episodes repeated across pages: %v", all)
	}
}

func TestLimitsAreDefaultedAndClamped(t *testing.T) {
	p := startReady(t, openSettings...)
	s := newScenario(t)
	// 205 episodes: one event each at its own camera.
	for i := range 205 {
		ev := s.event(i, "info", 0)
		ev.Camera = fmt.Sprintf("cam-%03d", i)
		s.publish(ev)
	}
	s.waitDrained()

	for query, want := range map[string]int{"": 50, "&limit=10": 10, "&limit=500": 200, "&limit=99999999999999999999999": 200} {
		if got := len(s.list(p, query)); got != want {
			t.Errorf("?site_id=…%s: %d episodes, want %d", query, got, want)
		}
	}
}

func TestMalformedListParametersAreRejected(t *testing.T) {
	p := startReady(t)
	for _, query := range []string{
		"limit=0", "limit=-1", "limit=abc", "limit=1.5",
		"cursor=not-a-cursor", "cursor=" + url.QueryEscape("eyJ2IjoyfQ"), // base64url of {"v":2}
		"kind=car_detected", "status=closed",
		"from=yesterday", "to=2026-08-04", "from=" + url.QueryEscape("2026-08-04 10:00:00Z"),
	} {
		var body struct {
			Error string `json:"error"`
		}
		p.getJSON("/episodes?"+query, http.StatusBadRequest, &body)
		if body.Error == "" {
			t.Errorf("?%s: no error message", query)
		}
	}
}

func TestAnEmptyListHasTheContractShape(t *testing.T) {
	p := startReady(t)
	status, body := p.get("/episodes?site_id=no-such-site")
	if status != http.StatusOK || strings.TrimSpace(body) != `{"episodes":[],"next_cursor":""}` {
		t.Errorf("status %d, body %s; want 200 and {\"episodes\":[],\"next_cursor\":\"\"}", status, body)
	}
}
