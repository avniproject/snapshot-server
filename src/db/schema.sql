CREATE TABLE IF NOT EXISTS snapshot_request (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    db_user      TEXT    NOT NULL,
    org_seq      INTEGER NOT NULL,
    mode         TEXT    NOT NULL CHECK (mode IN ('normal', 'clean')),
    state        TEXT    NOT NULL CHECK (state IN ('requested', 'in_progress', 'partial', 'ready', 'failed', 'cancelled')) DEFAULT 'requested',
    created_at   INTEGER NOT NULL DEFAULT (unixepoch()),
    started_at   INTEGER,
    finished_at  INTEGER,
    requested_by TEXT,
    UNIQUE (db_user, org_seq)
);

CREATE TABLE IF NOT EXISTS snapshot_user_job (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id    INTEGER NOT NULL REFERENCES snapshot_request(id) ON DELETE CASCADE,
    username      TEXT    NOT NULL,
    db_user       TEXT    NOT NULL,
    state         TEXT    NOT NULL CHECK (state IN ('queued', 'in_progress', 'ready', 'failed', 'cancelled')) DEFAULT 'queued',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error    TEXT,
    started_at    INTEGER,
    finished_at   INTEGER,
    resume_cursor TEXT,
    s3_key        TEXT,
    sha256        TEXT,
    size_bytes    INTEGER,
    worker_id     TEXT,
    locked_at     INTEGER,
    UNIQUE (request_id, username)
);

CREATE INDEX IF NOT EXISTS idx_user_job_state    ON snapshot_user_job (state);
CREATE INDEX IF NOT EXISTS idx_user_job_request  ON snapshot_user_job (request_id);
CREATE INDEX IF NOT EXISTS idx_request_db_user   ON snapshot_request (db_user);

CREATE TABLE IF NOT EXISTS snapshot_schedule (
    db_user         TEXT PRIMARY KEY,
    cron_expr       TEXT NOT NULL,
    immediate_users TEXT,
    last_run_at     INTEGER
);
