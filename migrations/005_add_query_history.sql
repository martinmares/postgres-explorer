-- Query history table
CREATE TABLE IF NOT EXISTS query_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    endpoint_id INTEGER NOT NULL,
    query_text TEXT NOT NULL,
    executed_at TEXT NOT NULL DEFAULT (datetime('now')),
    status TEXT NOT NULL, -- 'success' or 'failed'
    duration_ms INTEGER, -- execution time in milliseconds
    FOREIGN KEY (endpoint_id) REFERENCES endpoints(id) ON DELETE CASCADE
);

-- Index for faster lookups per endpoint
CREATE INDEX IF NOT EXISTS idx_query_history_endpoint ON query_history(endpoint_id, executed_at DESC);
