// Package settle runs the loop that settles open episodes once they have been
// silent long enough.
package settle

import (
	"context"
	"log/slog"
	"time"

	"panoptes/internal/store"
)

// interval is how often the loop looks for due episodes. An episode settles
// at most this much later than it became due.
const interval = 250 * time.Millisecond

// batch is how many due episodes one sweep takes on.
const batch = 100

// txTimeout bounds one settle transaction, which runs to the end even when
// shutdown begins.
const txTimeout = 10 * time.Second

// Run sweeps for due episodes until ctx ends, and settles each in its own
// transaction. The episode a sweep is settling when shutdown begins is
// finished; the rest wait for the next start.
func Run(ctx context.Context, st *store.Store, log *slog.Logger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		ids, err := st.DueEpisodes(ctx, batch)
		if err != nil {
			if ctx.Err() == nil {
				log.Warn("settle sweep failed", "error", err.Error())
			}
			continue
		}
		for _, id := range ids {
			if ctx.Err() != nil {
				return
			}
			txCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), txTimeout)
			ep, settled, err := st.Settle(txCtx, id)
			cancel()
			switch {
			case err != nil:
				log.Error("settle failed", "episode_id", id.String(), "error", err.Error())
			case settled:
				log.Info("episode settled", "episode_id", id.String(),
					"site_id", ep.Key.SiteID, "camera_id", ep.Key.CameraID, "kind", ep.Key.Kind,
					"close_reason", *ep.CloseReason, "event_count", ep.EventCount)
			}
		}
	}
}
