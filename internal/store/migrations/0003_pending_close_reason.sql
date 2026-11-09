-- An open episode that another episode of its key follows has ended by device
-- time; it records why it will settle, so that the settle step only has to
-- wait for its silence. Null while nothing follows it, and on settled episodes.
ALTER TABLE episodes
    ADD COLUMN pending_close_reason text CHECK (pending_close_reason IN ('quiet', 'max_duration'));
