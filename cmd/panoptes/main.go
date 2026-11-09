// Command panoptes correlates camera detection events into episodes.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"panoptes/internal/app"
	"panoptes/internal/config"
	"panoptes/internal/crash"
)

func main() {
	os.Exit(run())
}

// run returns the process exit code: 0 after a clean shutdown, 1 when the
// service failed, 2 when the configuration is invalid. A crash point exits
// with 3 from wherever it is.
func run() int {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.Load(os.Getenv)
	if err != nil {
		log.Error("invalid configuration", "error", err.Error())
		return 2
	}
	if err := crash.Arm(cfg.CrashAt); err != nil {
		log.Error("invalid configuration", "error", "PANOPTES_CRASH_AT: "+err.Error())
		return 2
	}

	// SIGTERM is what Docker and orchestrators send; SIGINT is Ctrl-C.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	if err := app.Run(ctx, cfg, log); err != nil {
		log.Error("stopped with an error", "error", err.Error())
		return 1
	}
	log.Info("stopped")
	return 0
}
