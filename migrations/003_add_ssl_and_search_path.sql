-- Add SSL mode and search_path columns to endpoints table
ALTER TABLE endpoints ADD COLUMN ssl_mode TEXT;
ALTER TABLE endpoints ADD COLUMN search_path TEXT;
