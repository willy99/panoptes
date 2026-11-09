package api

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"time"
	"uuid"

	"panoptes/internal/store"
)

// cursorVersion lets a later release change the cursor's contents and still
// recognise, and refuse, a cursor from before.
const cursorVersion = 1

// cursorJSON is what a cursor holds: the place of the last episode on the
// page. Clients see it only as base64url, which they must not interpret.
type cursorJSON struct {
	Version   int       `json:"v"`
	FirstSeen time.Time `json:"f"`
	ID        uuid.UUID `json:"i"`
}

func encodeCursor(p store.Position) string {
	b, _ := json.Marshal(cursorJSON{Version: cursorVersion, FirstSeen: p.FirstSeen.UTC(), ID: p.ID})
	return base64.RawURLEncoding.EncodeToString(b)
}

var errBadCursor = errors.New("cursor is not one this service returned")

func decodeCursor(s string) (store.Position, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return store.Position{}, errBadCursor
	}
	var c cursorJSON
	if err := json.Unmarshal(b, &c); err != nil || c.Version != cursorVersion || c.FirstSeen.IsZero() || c.ID == (uuid.UUID{}) {
		return store.Position{}, errBadCursor
	}
	return store.Position{FirstSeen: c.FirstSeen, ID: c.ID}, nil
}
