-- The list query without a site filter: newest first, id as the tiebreak.
-- With a site filter it uses episodes_by_site.
CREATE INDEX episodes_by_first_seen ON episodes (first_seen DESC, id DESC);
