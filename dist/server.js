import { createWriteStream } from "node:fs";
import { mkdir, rm } from "node:fs/promises";
import { dirname, join } from "node:path";
import { pipeline } from "node:stream/promises";
import Fastify from "fastify";
import cors from "@fastify/cors";
import multipart from "@fastify/multipart";
import { config } from "./config.js";
import { MediaStore } from "./db.js";
import { R2Storage } from "./r2.js";
import { SupabaseBridge } from "./supabase.js";
import { MediaJobWorker } from "./worker.js";
import { ensureDir, getFileExtension, nowIso, slugifySegment } from "./utils.js";
await ensureDir(dirname(config.mediaDbPath));
await ensureDir(config.mediaWorkDir);
const serviceStartedAt = new Date();
const app = Fastify({
    logger: true,
    bodyLimit: config.maxUploadBytes,
});
const store = new MediaStore(config.mediaDbPath);
const supabase = new SupabaseBridge(config.supabaseUrl, config.supabaseAnonKey, config.supabaseServiceRoleKey);
const r2 = new R2Storage({
    endpoint: config.r2Endpoint,
    accessKeyId: config.r2AccessKeyId,
    secretAccessKey: config.r2SecretAccessKey,
    bucketName: config.r2BucketName,
    publicBaseUrl: config.r2PublicUrl,
});
const worker = new MediaJobWorker({
    store,
    supabase,
    r2,
    logger: app.log,
});
await app.register(cors, {
    origin(origin, callback) {
        if (!origin) {
            callback(null, true);
            return;
        }
        callback(null, config.allowedOrigins.includes(origin));
    },
    methods: ["GET", "POST", "OPTIONS"],
    allowedHeaders: ["Authorization", "Content-Type"],
});
await app.register(multipart, {
    limits: {
        fileSize: config.maxUploadBytes,
        files: 2,
    },
});
function getAuthContext(request) {
    return request.authContext;
}
app.addHook("onRequest", async (request, reply) => {
    if (["/", "/uptime", "/uptime.json", "/health"].includes(request.url)) {
        return;
    }
    const authHeader = request.headers.authorization;
    if (!authHeader?.startsWith("Bearer ")) {
        reply.code(401);
        throw new Error("Unauthorized");
    }
    const accessToken = authHeader.replace(/^Bearer\s+/i, "").trim();
    const authContext = await supabase.authenticate(accessToken);
    request.authContext = authContext;
});
app.get("/health", async () => ({
    ok: true,
    now: nowIso(),
}));
app.get("/", async (_request, reply) => {
    reply.type("text/html; charset=utf-8");
    return renderUptimePage();
});
app.get("/uptime", async (_request, reply) => {
    reply.type("text/html; charset=utf-8");
    return renderUptimePage();
});
app.get("/uptime.json", async () => {
    const summary = store.getSummary();
    return {
        ok: true,
        service: "voiplay-media-service",
        startedAt: serviceStartedAt.toISOString(),
        uptimeSeconds: Math.floor((Date.now() - serviceStartedAt.getTime()) / 1000),
        summary,
        generatedAt: nowIso(),
    };
});
app.post("/api/media-jobs", async (request, reply) => {
    const auth = getAuthContext(request);
    if (!auth) {
        reply.code(401);
        return { error: "Unauthorized" };
    }
    const jobId = store.newJobId();
    const jobRoot = join(config.mediaWorkDir, jobId);
    const inputDir = join(jobRoot, "input");
    await mkdir(inputDir, { recursive: true });
    const fields = new Map();
    let videoPath = null;
    let videoOriginalName = null;
    let videoExt = null;
    let subtitlePath = null;
    let subtitleExt = null;
    let subtitleOriginalName = null;
    try {
        for await (const part of request.parts()) {
            if (part.type === "file") {
                if (part.fieldname === "video") {
                    videoOriginalName = part.filename || "video.bin";
                    videoExt = getFileExtension(videoOriginalName);
                    videoPath = join(inputDir, `video.${videoExt}`);
                    await saveMultipartFile(part.file, videoPath);
                    continue;
                }
                if (part.fieldname === "subtitle") {
                    subtitleOriginalName = part.filename || "subtitle.bin";
                    subtitleExt = getFileExtension(subtitleOriginalName);
                    subtitlePath = join(inputDir, `subtitle.${subtitleExt}`);
                    await saveMultipartFile(part.file, subtitlePath);
                    continue;
                }
                await part.toBuffer();
                continue;
            }
            fields.set(part.fieldname, String(part.value));
        }
        if (!videoPath || !videoExt || !videoOriginalName) {
            reply.code(400);
            return { error: "Video file is required" };
        }
        const episodeId = fields.get("episodeId");
        if (!episodeId) {
            reply.code(400);
            return { error: "episodeId is required" };
        }
        const episode = await supabase.getEpisodeContext(episodeId);
        supabase.assertEpisodeAccess(auth, episode);
        const r2Prefix = buildR2Prefix(episode.channelName, episode.contentTitle, episode.episodeNumber);
        const job = store.createJob({
            id: jobId,
            episode_id: episode.episodeId,
            content_id: episode.contentId,
            channel_id: episode.channelId,
            owner_user_id: episode.ownerUserId,
            requested_by: auth.userId,
            requested_by_name: auth.fullName,
            episode_number: episode.episodeNumber,
            channel_name: episode.channelName,
            content_title: episode.contentTitle,
            episode_title: episode.episodeTitle,
            source_video_path: videoPath,
            source_video_ext: videoExt,
            source_video_original_name: videoOriginalName,
            source_subtitle_path: subtitlePath,
            source_subtitle_ext: subtitleExt,
            source_subtitle_original_name: subtitleOriginalName,
            r2_prefix: r2Prefix,
            source_video_object_key: null,
            source_subtitle_object_key: null,
            stream_master_object_key: null,
            status: "queued",
            phase: "queued",
            cancel_requested: 0,
            cancel_reason: null,
            failure_reason: null,
            current_branch_index: 0,
            planned_branch_count: 0,
            completed_branch_count: 0,
            duration_seconds: null,
            video_bitrate: null,
            video_width: null,
            video_height: null,
            video_codec: null,
            audio_codec: null,
            elapsed_seconds: 0,
            estimated_completion_at: null,
            last_heartbeat_at: nowIso(),
            started_at: null,
            completed_at: null,
            result_video_url: null,
            result_subtitle_url: null,
            result_stream_url: null,
            result_hls_size_bytes: null,
            ffprobe_json: null,
        });
        store.addEvent(job.id, "info", "Job queued", {
            path: r2Prefix,
            requester: auth.userId,
        });
        worker.schedule();
        reply.code(201);
        return serializeJob(job.id, auth);
    }
    catch (error) {
        await rm(jobRoot, { recursive: true, force: true });
        throw error;
    }
});
app.get("/api/media-jobs", async (request) => {
    const auth = getAuthContext(request);
    const jobs = store.listJobs({
        ownerUserId: auth.isAdmin ? undefined : auth.userId,
        limit: 200,
    });
    return {
        jobs: jobs.map((job) => serializeJobRecord(job)),
    };
});
app.get("/api/media-jobs/summary", async (request) => {
    const auth = getAuthContext(request);
    return store.getSummary(auth.isAdmin ? undefined : auth.userId);
});
app.get("/api/media-jobs/:id", async (request, reply) => {
    const auth = getAuthContext(request);
    const params = request.params;
    const job = store.getJob(params.id);
    if (!job) {
        reply.code(404);
        return { error: "Job not found" };
    }
    assertJobAccess(auth, job);
    return serializeJob(params.id, auth);
});
app.post("/api/media-jobs/:id/cancel", async (request, reply) => {
    const auth = getAuthContext(request);
    const params = request.params;
    const body = (request.body || {}) || {};
    const job = store.getJob(params.id);
    if (!job) {
        reply.code(404);
        return { error: "Job not found" };
    }
    assertJobAccess(auth, job);
    const updatedJob = job.status === "queued"
        ? store.cancelQueuedJob(job.id, body.reason || "Cancelled by user")
        : store.markCancelRequested(job.id, body.reason || "Cancelled by user");
    store.addEvent(job.id, "warn", "Cancel requested", {
        by: auth.userId,
        reason: body.reason || null,
    });
    worker.requestCancel(job.id);
    return serializeJobRecord(updatedJob);
});
app.setErrorHandler((error, _request, reply) => {
    const message = error instanceof Error ? error.message : "Internal server error";
    const statusCode = message === "Unauthorized" ? 401 : message === "Forbidden" ? 403 : 500;
    if (!reply.sent) {
        reply.code(statusCode).send({
            error: message,
        });
    }
});
worker.start();
await app.listen({
    host: "0.0.0.0",
    port: config.port,
});
function serializeJob(jobId, auth) {
    const job = store.getJob(jobId);
    if (!job) {
        throw new Error("Job not found");
    }
    assertJobAccess(auth, job);
    return {
        job: serializeJobRecord(job),
        branches: store.getBranches(jobId),
        events: store.getEvents(jobId),
    };
}
function serializeJobRecord(job) {
    return {
        ...job,
        r2_base_path: job.r2_prefix,
    };
}
function assertJobAccess(auth, job) {
    if (auth.isAdmin)
        return;
    if (job.owner_user_id !== auth.userId) {
        throw new Error("Forbidden");
    }
}
async function saveMultipartFile(stream, targetPath) {
    await pipeline(stream, createWriteStream(targetPath));
}
function buildR2Prefix(channelName, contentTitle, episodeNumber) {
    const channelSegment = slugifySegment(channelName, "channel");
    const contentSegment = slugifySegment(contentTitle, "content");
    const episodeSegment = `episode-${String(episodeNumber).padStart(2, "0")}`;
    return `${channelSegment}/${contentSegment}/${episodeSegment}`;
}
function renderUptimePage() {
    const summary = store.getSummary();
    const uptimeSeconds = Math.floor((Date.now() - serviceStartedAt.getTime()) / 1000);
    const activeJob = summary.activeJobId ? store.getJob(summary.activeJobId) : null;
    return `<!doctype html>
<html lang="uz">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>VoiPlay Media Service Uptime</title>
    <style>
      :root {
        color-scheme: light;
        --bg: #f5f7fb;
        --card: #ffffff;
        --text: #111827;
        --muted: #6b7280;
        --line: #e5e7eb;
        --brand: #0f766e;
        --brand-soft: #ccfbf1;
        --ok: #15803d;
        --warn: #b45309;
        --danger: #b91c1c;
      }
      * { box-sizing: border-box; }
      body {
        margin: 0;
        font-family: "Segoe UI", system-ui, sans-serif;
        background:
          radial-gradient(circle at top left, rgba(15, 118, 110, 0.16), transparent 28%),
          linear-gradient(180deg, #f8fafc 0%, var(--bg) 100%);
        color: var(--text);
      }
      .wrap {
        max-width: 1080px;
        margin: 0 auto;
        padding: 32px 20px 56px;
      }
      .hero {
        display: flex;
        flex-wrap: wrap;
        justify-content: space-between;
        gap: 16px;
        align-items: center;
        margin-bottom: 24px;
      }
      .title {
        margin: 0;
        font-size: 32px;
        font-weight: 800;
      }
      .subtitle {
        margin: 8px 0 0;
        color: var(--muted);
        font-size: 14px;
      }
      .pill {
        display: inline-flex;
        align-items: center;
        gap: 8px;
        background: var(--brand-soft);
        color: var(--brand);
        padding: 10px 14px;
        border-radius: 999px;
        font-weight: 700;
        font-size: 14px;
      }
      .grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
        gap: 14px;
        margin-bottom: 24px;
      }
      .card {
        background: var(--card);
        border: 1px solid var(--line);
        border-radius: 18px;
        padding: 18px;
        box-shadow: 0 10px 30px rgba(15, 23, 42, 0.04);
      }
      .label {
        margin: 0;
        color: var(--muted);
        font-size: 13px;
      }
      .value {
        margin: 12px 0 0;
        font-size: 30px;
        font-weight: 800;
      }
      .good { color: var(--ok); }
      .warn { color: var(--warn); }
      .danger { color: var(--danger); }
      .section {
        display: grid;
        grid-template-columns: 1.2fr 0.8fr;
        gap: 14px;
      }
      .details {
        display: grid;
        gap: 12px;
      }
      .row {
        display: flex;
        justify-content: space-between;
        gap: 12px;
        padding: 12px 0;
        border-bottom: 1px solid var(--line);
      }
      .row:last-child { border-bottom: 0; }
      .row .left {
        color: var(--muted);
      }
      .row .right {
        text-align: right;
        font-weight: 600;
      }
      code {
        font-family: Consolas, monospace;
        font-size: 12px;
        background: #f3f4f6;
        padding: 2px 6px;
        border-radius: 6px;
      }
      @media (max-width: 860px) {
        .section {
          grid-template-columns: 1fr;
        }
      }
    </style>
  </head>
  <body>
    <div class="wrap">
      <div class="hero">
        <div>
          <h1 class="title">VoiPlay Media Service</h1>
          <p class="subtitle">Render uptime sahifasi · navbatlar, jarayon va umumiy tugallangan joblar</p>
        </div>
        <div class="pill">Online · ${escapeHtml(nowIso())}</div>
      </div>

      <div class="grid">
        <div class="card">
          <p class="label">Service uptime</p>
          <p class="value good">${escapeHtml(formatUptime(uptimeSeconds))}</p>
        </div>
        <div class="card">
          <p class="label">Navbatlar soni</p>
          <p class="value warn">${summary.queued}</p>
        </div>
        <div class="card">
          <p class="label">Jarayondagi joblar</p>
          <p class="value">${summary.running}</p>
        </div>
        <div class="card">
          <p class="label">Muvaffaqiyatli tugagan loyihalar</p>
          <p class="value good">${summary.completed}</p>
        </div>
        <div class="card">
          <p class="label">Jami joblar</p>
          <p class="value">${summary.total}</p>
        </div>
        <div class="card">
          <p class="label">Xato yoki bekor qilingan</p>
          <p class="value danger">${summary.failed + summary.cancelled}</p>
        </div>
      </div>

      <div class="section">
        <div class="card">
          <p class="label">Service tafsilotlari</p>
          <div class="details">
            <div class="row">
              <div class="left">Boshlangan vaqt</div>
              <div class="right">${escapeHtml(serviceStartedAt.toISOString())}</div>
            </div>
            <div class="row">
              <div class="left">Oldest queued</div>
              <div class="right">${escapeHtml(summary.oldestQueuedAt || "—")}</div>
            </div>
            <div class="row">
              <div class="left">Health endpoint</div>
              <div class="right"><code>/health</code></div>
            </div>
            <div class="row">
              <div class="left">JSON status</div>
              <div class="right"><code>/uptime.json</code></div>
            </div>
          </div>
        </div>

        <div class="card">
          <p class="label">Aktiv job</p>
          <div class="details">
            <div class="row">
              <div class="left">Job ID</div>
              <div class="right"><code>${escapeHtml(activeJob?.id || "—")}</code></div>
            </div>
            <div class="row">
              <div class="left">Kontent</div>
              <div class="right">${escapeHtml(activeJob?.content_title || "—")}</div>
            </div>
            <div class="row">
              <div class="left">Holat</div>
              <div class="right">${escapeHtml(activeJob?.status || "Idle")}</div>
            </div>
            <div class="row">
              <div class="left">Phase</div>
              <div class="right">${escapeHtml(activeJob?.phase || "Kutish rejimi")}</div>
            </div>
          </div>
        </div>
      </div>
    </div>
  </body>
</html>`;
}
function formatUptime(totalSeconds) {
    const days = Math.floor(totalSeconds / 86400);
    const hours = Math.floor((totalSeconds % 86400) / 3600);
    const minutes = Math.floor((totalSeconds % 3600) / 60);
    const seconds = totalSeconds % 60;
    const parts = [
        days > 0 ? `${days} kun` : null,
        hours > 0 || days > 0 ? `${hours} soat` : null,
        minutes > 0 || hours > 0 || days > 0 ? `${minutes} daqiqa` : null,
        `${seconds} soniya`,
    ].filter(Boolean);
    return parts.join(" ");
}
function escapeHtml(value) {
    return value
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
}
