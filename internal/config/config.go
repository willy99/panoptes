// Package config reads the service settings from the environment once, at startup.
package config

import (
	"errors"
	"fmt"
	"time"
)

// Config holds every setting the service reads. The names of the environment
// variables are part of the brief's contract, except EPISODE_SETTLE_GRACE, which is ours.
type Config struct {
	QuietWindow time.Duration // EPISODE_QUIET_WINDOW: longest device-time gap inside one episode, and the silence that settles it
	MaxDuration time.Duration // EPISODE_MAX_DURATION: longest device-time span of one chapter
	SettleGrace time.Duration // EPISODE_SETTLE_GRACE: silence that settles an episode that has ended by device time
	NATSURL     string        // NATS_URL
	DatabaseURL string        // DATABASE_URL
	HTTPAddr    string        // HTTP_ADDR: API, health and metrics listener
	CrashAt     string        // PANOPTES_CRASH_AT: test only, the crash point to exit at
}

// Load reads the settings through getenv and reports every invalid variable at once,
// so a misconfigured deployment is fixed in one round rather than one error at a time.
func Load(getenv func(string) string) (Config, error) {
	var errs []error

	duration := func(name string, def time.Duration, zeroAllowed bool) time.Duration {
		raw := getenv(name)
		if raw == "" {
			return def
		}
		d, err := time.ParseDuration(raw)
		switch {
		case err != nil:
			errs = append(errs, fmt.Errorf("%s: %q is not a duration such as 60s or 10m", name, raw))
		case zeroAllowed && d < 0:
			errs = append(errs, fmt.Errorf("%s: must be zero or more, got %q", name, raw))
		case !zeroAllowed && d <= 0:
			errs = append(errs, fmt.Errorf("%s: must be greater than zero, got %q", name, raw))
		}
		return d
	}
	text := func(name, def string) string {
		if v := getenv(name); v != "" {
			return v
		}
		return def
	}

	cfg := Config{
		QuietWindow: duration("EPISODE_QUIET_WINDOW", 60*time.Second, false),
		MaxDuration: duration("EPISODE_MAX_DURATION", 10*time.Minute, false),
		SettleGrace: duration("EPISODE_SETTLE_GRACE", time.Second, true),
		NATSURL:     text("NATS_URL", "nats://nats:4222"),
		DatabaseURL: getenv("DATABASE_URL"),
		HTTPAddr:    text("HTTP_ADDR", ":8080"),
		CrashAt:     getenv("PANOPTES_CRASH_AT"),
	}
	if cfg.DatabaseURL == "" {
		errs = append(errs, errors.New("DATABASE_URL: required, for example postgres://user@host:5432/db"))
	}
	return cfg, errors.Join(errs...)
}
