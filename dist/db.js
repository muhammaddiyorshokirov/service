import Database from "better-sqlite3";
import { randomUUID } from "node:crypto";
import { nowIso } from "./utils.js";
export class MediaStore {
    db;
    constructor(dbPath) {
        this.db = new Database(dbPath);
        this.db.pragma("journal_mode = WAL");
        this.db.pragma("foreign_keys = ON");
        this.migrate();
    }
    migrate() {
        this.db.exec(`
      CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY,
        episode_id TEXT NOT NULL,
        content_id TEXT NOT NULL,
        channel_id TEXT NOT NULL,
        owner_user_id TEXT NOT NULL,
        requested_by TEXT NOT NULL,
        requested_by_name TEXT,
        episode_number INTEGER NOT NULL,
        channel_name TEXT,
        content_title TEXT,
        episode_title TEXT,
        source_video_path TEXT NOT NULL,
        source_video_ext TEXT NOT NULL,
        source_video_original_name TEXT NOT NULL,
        source_subtitle_path TEXT,
        source_subtitle_ext TEXT,
        source_subtitle_original_name TEXT,
        r2_prefix TEXT NOT NULL,
        source_video_object_key TEXT,
        source_subtitle_object_key TEXT,
        stream_master_object_key TEXT,
        status TEXT NOT NULL,
        phase TEXT NOT NULL,
        cancel_requested INTEGER NOT NULL DEFAULT 0,
        cancel_reason TEXT,
        failure_reason TEXT,
        current_branch_index INTEGER,
        planned_branch_count INTEGER NOT NULL DEFAULT 0,
        completed_branch_count INTEGER NOT NULL DEFAULT 0,
        duration_seconds REAL,
        video_bitrate INTEGER,
        video_width INTEGER,
        video_height INTEGER,
        video_codec TEXT,
        audio_codec TEXT,
        elapsed_seconds REAL,
        estimated_completion_at TEXT,
        last_heartbeat_at TEXT,
        started_at TEXT,
        completed_at TEXT,
        result_video_url TEXT,
        result_subtitle_url TEXT,
        result_stream_url TEXT,
        result_hls_size_bytes INTEGER,
        ffprobe_json TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS job_branches (
        id TEXT PRIMARY KEY,
        job_id TEXT NOT NULL,
        branch_index INTEGER NOT NULL,
        start_seconds REAL NOT NULL,
        end_seconds REAL NOT NULL,
        target_duration_seconds REAL NOT NULL,
        status TEXT NOT NULL,
        resume_from_seconds REAL NOT NULL DEFAULT 0,
        duration_completed_seconds REAL NOT NULL DEFAULT 0,
        segment_count INTEGER NOT NULL DEFAULT 0,
        playlist_path TEXT,
        last_error TEXT,
        started_at TEXT,
        completed_at TEXT,
        updated_at TEXT NOT NULL,
        UNIQUE(job_id, branch_index),
        FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
      );

      CREATE TABLE IF NOT EXISTS job_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id TEXT NOT NULL,
        level TEXT NOT NULL,
        message TEXT NOT NULL,
        data_json TEXT,
        created_at TEXT NOT NULL,
        FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
      );

      CREATE INDEX IF NOT EXISTS idx_jobs_status_created_at ON jobs(status, created_at);
      CREATE INDEX IF NOT EXISTS idx_jobs_owner_created_at ON jobs(owner_user_id, created_at);
      CREATE INDEX IF NOT EXISTS idx_jobs_channel_created_at ON jobs(channel_id, created_at);
      CREATE INDEX IF NOT EXISTS idx_job_branches_job_index ON job_branches(job_id, branch_index);
    `);
    }
    createJob(input) {
        const createdAt = nowIso();
        const payload = { ...input, created_at: createdAt, updated_at: createdAt };
        const columns = Object.keys(payload);
        const statement = this.db.prepare(`
      INSERT INTO jobs (${columns.join(", ")})
      VALUES (${columns.map((column) => `@${column}`).join(", ")})
    `);
        statement.run(payload);
        return this.getJob(payload.id);
    }
    getJob(id) {
        return this.db.prepare("SELECT * FROM jobs WHERE id = ?").get(id);
    }
    getBranches(jobId) {
        return this.db
            .prepare("SELECT * FROM job_branches WHERE job_id = ? ORDER BY branch_index")
            .all(jobId);
    }
    getEvents(jobId, limit = 25) {
        return this.db
            .prepare("SELECT * FROM job_events WHERE job_id = ? ORDER BY id DESC LIMIT ?")
            .all(jobId, limit);
    }
    addEvent(jobId, level, message, data) {
        this.db
            .prepare(`
        INSERT INTO job_events (job_id, level, message, data_json, created_at)
        VALUES (?, ?, ?, ?, ?)
      `)
            .run(jobId, level, message, data == null ? null : JSON.stringify(data), nowIso());
    }
    updateJob(id, patch) {
        const entries = Object.entries({ ...patch, updated_at: nowIso() }).filter(([, value]) => value !== undefined);
        if (!entries.length)
            return this.getJob(id);
        const sql = `UPDATE jobs SET ${entries.map(([key]) => `${key} = @${key}`).join(", ")} WHERE id = @id`;
        this.db.prepare(sql).run({ id, ...Object.fromEntries(entries) });
        return this.getJob(id);
    }
    insertBranches(jobId, branches) {
        const statement = this.db.prepare(`
      INSERT INTO job_branches (
        id, job_id, branch_index, start_seconds, end_seconds, target_duration_seconds,
        status, resume_from_seconds, duration_completed_seconds, segment_count, playlist_path,
        last_error, started_at, completed_at, updated_at
      )
      VALUES (
        @id, @job_id, @branch_index, @start_seconds, @end_seconds, @target_duration_seconds,
        @status, @resume_from_seconds, @duration_completed_seconds, @segment_count, @playlist_path,
        @last_error, @started_at, @completed_at, @updated_at
      )
    `);
        const transaction = this.db.transaction((items) => {
            for (const branch of items) {
                statement.run({
                    ...branch,
                    id: `${jobId}:${branch.branch_index}`,
                    updated_at: nowIso(),
                });
            }
        });
        transaction(branches);
        return this.getBranches(jobId);
    }
    updateBranch(jobId, branchIndex, patch) {
        const id = `${jobId}:${branchIndex}`;
        const entries = Object.entries({ ...patch, updated_at: nowIso() }).filter(([, value]) => value !== undefined);
        if (!entries.length)
            return this.getBranch(jobId, branchIndex);
        const sql = `UPDATE job_branches SET ${entries.map(([key]) => `${key} = @${key}`).join(", ")} WHERE id = @id`;
        this.db.prepare(sql).run({ id, ...Object.fromEntries(entries) });
        return this.getBranch(jobId, branchIndex);
    }
    getBranch(jobId, branchIndex) {
        return this.db
            .prepare("SELECT * FROM job_branches WHERE job_id = ? AND branch_index = ?")
            .get(jobId, branchIndex);
    }
    listJobs(options = {}) {
        const clauses = [];
        const params = [];
        if (options.ownerUserId) {
            clauses.push("owner_user_id = ?");
            params.push(options.ownerUserId);
        }
        if (options.status) {
            clauses.push("status = ?");
            params.push(options.status);
        }
        const where = clauses.length ? `WHERE ${clauses.join(" AND ")}` : "";
        const limit = Math.min(options.limit ?? 100, 250);
        return this.db
            .prepare(`SELECT * FROM jobs ${where} ORDER BY created_at DESC LIMIT ?`)
            .all(...params, limit);
    }
    getNextQueuedJob() {
        return this.db
            .prepare("SELECT * FROM jobs WHERE status = 'queued' ORDER BY created_at ASC LIMIT 1")
            .get();
    }
    requeueInterruptedJobs() {
        const now = nowIso();
        this.db
            .prepare(`
        UPDATE jobs
        SET status = CASE WHEN cancel_requested = 1 THEN 'cancelled' ELSE 'queued' END,
            phase = CASE WHEN cancel_requested = 1 THEN 'cancelled' ELSE 'queued' END,
            completed_at = CASE WHEN cancel_requested = 1 THEN COALESCE(completed_at, @now) ELSE completed_at END,
            updated_at = @now
        WHERE status IN ('running', 'uploading', 'cancelling')
      `)
            .run({ now });
    }
    cancelQueuedJob(jobId, reason) {
        this.db
            .prepare(`
        UPDATE jobs
        SET status = 'cancelled',
            phase = 'cancelled',
            cancel_requested = 1,
            cancel_reason = ?,
            completed_at = ?,
            updated_at = ?
        WHERE id = ? AND status = 'queued'
      `)
            .run(reason, nowIso(), nowIso(), jobId);
        return this.getJob(jobId);
    }
    markCancelRequested(jobId, reason) {
        this.db
            .prepare(`
        UPDATE jobs
        SET cancel_requested = 1,
            cancel_reason = ?,
            status = CASE WHEN status = 'queued' THEN 'cancelled' ELSE 'cancelling' END,
            phase = CASE WHEN status = 'queued' THEN 'cancelled' ELSE 'cancelling' END,
            completed_at = CASE WHEN status = 'queued' THEN ? ELSE completed_at END,
            updated_at = ?
        WHERE id = ?
      `)
            .run(reason, nowIso(), nowIso(), jobId);
        return this.getJob(jobId);
    }
    getSummary(ownerUserId) {
        const rows = this.listJobs({ ownerUserId, limit: 500 });
        const queuedRows = rows.filter((row) => row.status === "queued");
        const oldestQueuedAt = queuedRows.length > 0
            ? queuedRows.reduce((oldest, row) => new Date(row.created_at).getTime() < new Date(oldest).getTime() ? row.created_at : oldest, queuedRows[0].created_at)
            : null;
        return {
            total: rows.length,
            queued: queuedRows.length,
            running: rows.filter((row) => row.status === "running" || row.status === "uploading" || row.status === "cancelling").length,
            completed: rows.filter((row) => row.status === "completed").length,
            failed: rows.filter((row) => row.status === "failed").length,
            cancelled: rows.filter((row) => row.status === "cancelled").length,
            oldestQueuedAt,
            activeJobId: rows.find((row) => ["running", "uploading", "cancelling"].includes(row.status))?.id ?? null,
        };
    }
    newJobId() {
        return randomUUID();
    }
    close() {
        this.db.close();
    }
}
