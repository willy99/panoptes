package e2e

import (
	"math/rand/v2"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
)

// These tests run with the harness's test settings: a quiet window of 1s, a
// max duration of 5s and a settle grace of 200ms.

// timelineFor returns n events for one key, half a second apart in device
// time with now and then a gap longer than the quiet window, so that the key
// forms several episodes and chapters.
func (s *scenario) timelineFor(rng *rand.Rand, camera, kind string, first, n int) []testEvent {
	var events []testEvent
	offset := time.Duration(0)
	for i := range n {
		e := s.event(first+i, []string{"info", "notice", "warning", "critical"}[rng.IntN(4)], offset)
		e.Camera, e.Kind = camera, kind
		events = append(events, e)
		offset += 500 * ms
		if rng.IntN(8) == 0 {
			offset += 1500 * ms
		}
	}
	return events
}

func TestTwoInstancesGiveTheSameResultAsOne(t *testing.T) {
	a, b := startReady(t), startReady(t)
	s := newScenario(t)
	rng := rand.New(rand.NewPCG(9, 9))

	// Four keys, twenty events each, every event published twice and some a
	// third time, all shuffled together.
	var all []testEvent
	n := 0
	for _, camera := range []string{"cam-1", "cam-2"} {
		for _, kind := range []string{"person_detected", "tamper"} {
			all = append(all, s.timelineFor(rng, camera, kind, n, 20)...)
			n += 20
		}
	}
	var deliveries []testEvent
	for _, e := range all {
		deliveries = append(deliveries, e, e)
		if rng.IntN(4) == 0 {
			deliveries = append(deliveries, e)
		}
	}
	rng.Shuffle(len(deliveries), func(i, j int) { deliveries[i], deliveries[j] = deliveries[j], deliveries[i] })
	s.publish(deliveries...)
	s.waitDrained()

	s.waitAllSettled(a)
	s.checkInvariants(a)
	s.checkInvariants(b) // both instances see the same database
}

// TestSoakKillsInstancesAtRandom keeps two instances under steady load and
// kills one of them with SIGKILL at a random moment, round after round, then
// checks the invariants. PANOPTES_SOAK_ROUNDS raises the number of rounds and
// PANOPTES_SOAK_SEED replays a run; the seed is logged.
func TestSoakKillsInstancesAtRandom(t *testing.T) {
	rounds := envInt(t, "PANOPTES_SOAK_ROUNDS", 4)
	seed := uint64(envInt(t, "PANOPTES_SOAK_SEED", int(time.Now().UnixNano()%1_000_000)))
	t.Logf("soak: %d rounds, seed %d (PANOPTES_SOAK_SEED=%d replays it)", rounds, seed, seed)
	rng := rand.New(rand.NewPCG(seed, seed))

	shortAckWait(t)
	instances := []*proc{startReady(t), startReady(t)}
	s := newScenario(t)
	s.extraDeliveries = anyRedelivery

	// The load: events for three keys, round robin, with a duplicate now and
	// then; enough of them to last through every round.
	var events []testEvent
	perKey := 100 * rounds
	keys := [][]testEvent{}
	for i, camera := range []string{"cam-1", "cam-2", "cam-3"} {
		keys = append(keys, s.timelineFor(rng, camera, "loitering", i*perKey, perKey))
	}
	for i := range perKey {
		for _, k := range keys {
			events = append(events, k[i])
		}
	}
	stop := make(chan struct{})
	var load sync.WaitGroup
	loadRng := rand.New(rand.NewPCG(seed, seed+1)) // the load's own: rand.Rand is not safe across goroutines
	load.Go(func() {
		for i, e := range events {
			select {
			case <-stop:
				return
			default:
			}
			s.publish(e)
			if i%7 == 0 {
				s.publish(events[loadRng.IntN(i+1)]) // a duplicate of an earlier event
			}
			time.Sleep(5 * time.Millisecond)
		}
	})

	for round := range rounds {
		time.Sleep(time.Duration(300+rng.IntN(900)) * time.Millisecond)
		i := rng.IntN(len(instances))
		t.Logf("round %d: killing instance %d", round, i)
		instances[i].kill()
		instances[i] = startReady(t)
	}
	close(stop)
	load.Wait()

	s.waitDrained()
	s.waitAllSettled(instances[0])
	s.checkInvariants(instances[0])
}

func envInt(t *testing.T, name string, def int) int {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		t.Fatalf("%s=%q: %v", name, v, err)
	}
	return n
}
