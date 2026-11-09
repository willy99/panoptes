-- How many corrections a settled episode has had: 0 when it settles, plus
-- one per late event that joins it (ADR-0005).
ALTER TABLE episodes ADD COLUMN revision integer NOT NULL DEFAULT 0;
