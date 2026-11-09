// Command demo publishes a scenario to a running Panoptes stack so that a
// reviewer can watch episodes form. It speaks only the public contract: it
// publishes events to analytics.event.v1.* and reads announcements from the
// episode stream, and it imports none of the service's packages.
//
// The scenario, for one site of its own:
//   - cam-01 person_detected: a busy camera for 11 minutes of device time, so
//     the max duration cuts it into two chapters; once the first chapter's
//     announcement arrives, a late event before the boundary corrects it;
//   - cam-02 loitering: two incidents five minutes apart, two episodes;
//   - cam-02 tamper: warnings and one critical, so the peak is critical;
//   - about one event in eight published twice, everything shuffled;
//   - four invalid messages.
//
// Device times are relative to the moment it runs, and -seed replays the same
// shuffles and duplicates.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type event struct {
	ID         string         `json:"event_id"`
	Site       string         `json:"site_id"`
	Camera     string         `json:"camera_id"`
	Kind       string         `json:"kind"`
	Severity   string         `json:"severity"`
	OccurredAt string         `json:"occurred_at"`
	Attributes map[string]any `json:"attributes"`
}

func main() {
	natsURL := flag.String("nats", envOr("NATS_URL", "nats://localhost:4222"), "NATS URL")
	seed := flag.Uint64("seed", uint64(time.Now().UnixNano()%1_000_000), "seed for the shuffles and duplicates")
	watch := flag.Duration("watch", 75*time.Second, "how long to print announcements after publishing; one quiet window and a little more")
	api := flag.String("api", "http://localhost:8080", "the API address to print in the hints")
	flag.Parse()

	if err := run(*natsURL, *seed, *watch, *api); err != nil {
		fmt.Fprintln(os.Stderr, "demo:", err)
		os.Exit(1)
	}
}

func run(natsURL string, seed uint64, watch time.Duration, api string) error {
	nc, err := nats.Connect(natsURL)
	if err != nil {
		return fmt.Errorf("connect to %s: %w", natsURL, err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	ctx := context.Background()

	rng := rand.New(rand.NewPCG(seed, seed))
	start := time.Now().UTC().Truncate(time.Second)
	site := fmt.Sprintf("demo-%s", start.Format("150405"))
	base := start.Add(-15 * time.Minute) // device times in the recent past
	n := 0
	ev := func(camera, kind, severity string, offset time.Duration) event {
		n++
		return event{
			ID: fmt.Sprintf("%s-%04d", site, n), Site: site, Camera: camera, Kind: kind, Severity: severity,
			OccurredAt: base.Add(offset).Format(time.RFC3339Nano),
			Attributes: map[string]any{"confidence": 0.5 + rng.Float64()/2, "track_id": rng.IntN(10000)},
		}
	}

	var events []event
	// A busy camera: every 20s for 11 minutes; the 10:00 boundary cuts it.
	for offset := time.Duration(0); offset <= 11*time.Minute; offset += 20 * time.Second {
		events = append(events, ev("cam-01", "person_detected", []string{"info", "notice", "warning"}[rng.IntN(3)], offset))
	}
	// Two loitering incidents, five minutes apart.
	for _, offset := range []time.Duration{0, 30 * time.Second, 45 * time.Second, 6 * time.Minute, 6*time.Minute + 15*time.Second} {
		events = append(events, ev("cam-02", "loitering", "notice", offset))
	}
	// Tamper warnings with one critical among them.
	for i, offset := range []time.Duration{2 * time.Minute, 2*time.Minute + 10*time.Second, 2*time.Minute + 20*time.Second} {
		events = append(events, ev("cam-02", "tamper", map[bool]string{true: "critical", false: "warning"}[i == 1], offset))
	}

	deliveries := append([]event(nil), events...)
	duplicates := 0
	for _, e := range events {
		if rng.IntN(8) == 0 {
			deliveries = append(deliveries, e)
			duplicates++
		}
	}
	rng.Shuffle(len(deliveries), func(i, j int) { deliveries[i], deliveries[j] = deliveries[j], deliveries[i] })

	// Listen before publishing, so that no announcement is missed.
	announcements, err := listen(ctx, js, site)
	if err != nil {
		return err
	}

	for _, e := range deliveries {
		if err := publish(ctx, js, e); err != nil {
			return err
		}
	}
	invalid := []struct{ subject, payload string }{
		{"analytics.event.v1." + site + ".cam-03.tamper", "not json"},
		{"analytics.event.v1." + site + ".cam-03.tamper", `{"site_id":"` + site + `","camera_id":"cam-03","kind":"tamper","severity":"info","occurred_at":"2026-08-04T10:00:00Z"}`},
		{"analytics.event.v1." + site + ".cam-03.car", `{"event_id":"` + site + `-bad-1","site_id":"` + site + `","camera_id":"cam-03","kind":"car","severity":"info","occurred_at":"2026-08-04T10:00:00Z"}`},
		{"analytics.event.v1." + site + ".cam-03.tamper", `{"event_id":"` + site + `-bad-2","site_id":"` + site + `","camera_id":"cam.03","kind":"tamper","severity":"info","occurred_at":"2026-08-04T10:00:00Z"}`},
	}
	for _, m := range invalid {
		if _, err := js.Publish(ctx, m.subject, []byte(m.payload)); err != nil {
			return fmt.Errorf("publish an invalid message: %w", err)
		}
	}
	fmt.Printf("site %s, seed %d: published %d events (%d of them twice, shuffled) and %d invalid messages\n",
		site, seed, len(events), duplicates, len(invalid))
	fmt.Println("cam-01 person_detected runs past the max duration; waiting for its first chapter to settle...")

	deadline := time.After(watch)
	late := ev("cam-01", "person_detected", "critical", 9*time.Minute+50*time.Second)
	lateSent := false
	for {
		select {
		case a := <-announcements:
			fmt.Println("  " + a)
			if !lateSent && strings.Contains(a, `"close_reason":"max_duration"`) {
				if err := publish(ctx, js, late); err != nil {
					return err
				}
				lateSent = true
				fmt.Println("published a late event at 09:50 into the settled first chapter: expect a correction with revision 1")
			}
		case <-deadline:
			fmt.Printf(`
Look at:
  curl '%[1]s/episodes?site_id=%[2]s'                     5 episodes, all settled after one quiet window
  curl '%[1]s/episodes?site_id=%[2]s&camera_id=cam-01'    two chapters: max_duration, then quiet
  curl -s %[1]s/metrics | grep episoder_invalid_events_total    up by 4
Announcements for this site: 5 on episode.settled.%[2]s.*, 1 on episode.corrected.%[2]s.*
Run it again with -seed %[3]d for the same shuffles and duplicates.
`, api, site, seed)
			return nil
		}
	}
}

// listen prints every announcement for the site as it arrives on the episode
// stream, read through an ordered consumer like any platform service would.
func listen(ctx context.Context, js jetstream.JetStream, site string) (<-chan string, error) {
	stream, err := js.StreamNameBySubject(ctx, "episode.settled."+site+".x")
	if err != nil {
		return nil, fmt.Errorf("find the episode stream (is Panoptes running?): %w", err)
	}
	consumer, err := js.OrderedConsumer(ctx, stream, jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{"episode.settled." + site + ".>", "episode.corrected." + site + ".>"},
		DeliverPolicy:  jetstream.DeliverNewPolicy,
	})
	if err != nil {
		return nil, err
	}
	out := make(chan string, 16)
	_, err = consumer.Consume(func(msg jetstream.Msg) {
		out <- msg.Subject() + " " + string(msg.Data())
	})
	return out, err
}

func publish(ctx context.Context, js jetstream.JetStream, e event) error {
	payload, err := json.Marshal(e)
	if err != nil {
		return err
	}
	subject := "analytics.event.v1." + e.Site + "." + e.Camera + "." + e.Kind
	if _, err := js.Publish(ctx, subject, payload); err != nil {
		return fmt.Errorf("publish %s: %w", e.ID, err)
	}
	return nil
}

func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}
