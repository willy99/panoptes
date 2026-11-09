-- Events, and the episodes they form.
--
-- An event row is written once, by the first delivery of its event_id, and
-- afterwards only the episode it belongs to can change. Episodes are derived:
-- their aggregates are recomputed from their events on every change and never
-- patched (ADR-0002).

CREATE TABLE episodes (
    id            uuid PRIMARY KEY,
    site_id       text NOT NULL,
    camera_id     text NOT NULL,
    kind          text NOT NULL,
    status        text NOT NULL CHECK (status IN ('open', 'settled')),
    first_seen    timestamptz NOT NULL,
    last_seen     timestamptz NOT NULL,
    event_count   integer NOT NULL CHECK (event_count > 0),
    peak_severity smallint NOT NULL CHECK (peak_severity BETWEEN 1 AND 4), -- rank: 1 info .. 4 critical
    last_arrival  timestamptz NOT NULL, -- latest arrival time among its events; settling measures silence from it
    close_reason  text CHECK (close_reason IN ('quiet', 'max_duration')),
    settled_at    timestamptz,
    CONSTRAINT settled_has_reason_and_time CHECK (
        (status = 'open' AND close_reason IS NULL AND settled_at IS NULL) OR
        (status = 'settled' AND close_reason IS NOT NULL AND settled_at IS NOT NULL)
    )
);

-- The ingest transaction reads a key's episodes in device-time order.
CREATE INDEX episodes_by_key ON episodes (site_id, camera_id, kind, first_seen);

-- The list query: one site, newest first, id as the tiebreak.
CREATE INDEX episodes_by_site ON episodes (site_id, first_seen DESC, id DESC);

CREATE TABLE events (
    -- Byte order, as in Go, so that ties in device time sort the same way in
    -- SQL and in code.
    event_id    text COLLATE "C" PRIMARY KEY,
    site_id     text NOT NULL,
    camera_id   text NOT NULL,
    kind        text NOT NULL,
    severity    smallint NOT NULL CHECK (severity BETWEEN 1 AND 4), -- rank, as on episodes
    occurred_at timestamptz NOT NULL, -- device time
    -- json, not jsonb: json keeps the device's text byte for byte, while jsonb
    -- reorders keys and rejects valid JSON such as "\u0000", which would make
    -- such an event fail on every delivery.
    attributes  json NOT NULL,
    arrived_at  timestamptz NOT NULL, -- arrival time, by the Postgres clock
    -- Null only inside the ingest transaction that stores the event: the row is
    -- inserted first, to detect a duplicate, and assigned to its episode after.
    episode_id  uuid REFERENCES episodes (id)
);

-- An episode's events in device-time order, and the foreign key's lookups.
CREATE INDEX events_by_episode ON events (episode_id, occurred_at, event_id);
