-- Announcements waiting to be published, written by the same transaction
-- that settles or corrects an episode (ADR-0004). The payload is a snapshot
-- as of that transaction. A relay publishes rows in id order with msg_id as
-- Nats-Msg-Id and marks them published; published rows are kept.
CREATE TABLE outbox (
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    episode_id   uuid NOT NULL REFERENCES episodes (id),
    subject      text NOT NULL,
    msg_id       text NOT NULL UNIQUE, -- settled:<episode_id> or corrected:<episode_id>:<revision>
    payload      json NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);

-- The relay's work list, and its check for an earlier unpublished row of the
-- same episode.
CREATE INDEX outbox_unpublished ON outbox (episode_id, id) WHERE published_at IS NULL;

-- The settle sweep: open episodes by how long ago their latest event arrived.
CREATE INDEX episodes_open_by_arrival ON episodes (last_arrival) WHERE status = 'open';
