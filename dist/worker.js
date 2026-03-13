import { spawn } from "node:child_process";
import { copyFile, readFile, rm, stat, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { config } from "./config.js";
import { buildBranchFfmpegArgs, buildMasterPlaylist, buildSubtitleConvertArgs, createSubtitlePlaylist, ffmpegBinaryPath, parsePlaylistFile, probeMedia, writePlaylist, } from "./hls.js";
import { ensureDir, getDirectorySize, nowIso, parsePlaylistSegments, removeDirIfExists, } from "./utils.js";
class CancelledError extends Error {
    constructor(message = "Job cancelled") {
        super(message);
        this.name = "CancelledError";
    }
}
export class MediaJobWorker {
    store;
    supabase;
    r2;
    logger;
    ticking = false;
    activeJobId = null;
    activeChild = null;
    constructor(input) {
        this.store = input.store;
        this.supabase = input.supabase;
        this.r2 = input.r2;
        this.logger = input.logger;
    }
    start() {
        this.store.requeueInterruptedJobs();
        this.schedule();
    }
    schedule() {
        setImmediate(() => {
            void this.tick();
        });
    }
    requestCancel(jobId) {
        if (this.activeJobId !== jobId || !this.activeChild)
            return;
        this.activeChild.kill("SIGTERM");
        setTimeout(() => {
            if (this.activeChild && this.activeJobId === jobId) {
                this.activeChild.kill("SIGKILL");
            }
        }, 5000).unref();
    }
    async tick() {
        if (this.ticking)
            return;
        this.ticking = true;
        try {
            const nextJob = this.store.getNextQueuedJob();
            if (!nextJob)
                return;
            await this.processJob(nextJob.id);
        }
        catch (error) {
            this.logger.error({ error }, "Media worker tick failed");
        }
        finally {
            this.ticking = false;
            if (this.store.getNextQueuedJob()) {
                this.schedule();
            }
        }
    }
    async processJob(jobId) {
        let job = this.store.getJob(jobId);
        if (!job)
            return;
        this.activeJobId = job.id;
        this.store.addEvent(job.id, "info", "Job started");
        job = this.store.updateJob(job.id, {
            status: "running",
            phase: "probing",
            started_at: job.started_at || nowIso(),
            last_heartbeat_at: nowIso(),
            failure_reason: null,
        });
        try {
            await this.throwIfCancelled(job.id);
            const workDir = join(config.mediaWorkDir, job.id);
            const hlsDir = join(workDir, "hls");
            const branchesDir = join(hlsDir, "branches");
            const subtitleDir = join(hlsDir, "subtitles");
            await ensureDir(branchesDir);
            const probe = await probeMedia(job.source_video_path);
            job = this.store.updateJob(job.id, {
                phase: "planning",
                duration_seconds: probe.durationSeconds,
                video_bitrate: probe.bitrate,
                video_width: probe.width,
                video_height: probe.height,
                video_codec: probe.videoCodec,
                audio_codec: probe.audioCodec,
                ffprobe_json: JSON.stringify(probe.raw),
            });
            await this.ensureBranches(job, probe);
            await this.processBranches(job.id);
            let subtitleUrl = null;
            let subtitleObjectKey = null;
            let subtitleFileName = null;
            let subtitleSizeBytes = null;
            if (job.source_subtitle_path) {
                job = this.store.updateJob(job.id, { phase: "subtitle" });
                const subtitleInfo = await this.prepareSubtitle(job, subtitleDir, probe.durationSeconds);
                subtitleUrl = subtitleInfo.publicUrl;
                subtitleObjectKey = subtitleInfo.objectKey;
                subtitleFileName = subtitleInfo.fileName;
                subtitleSizeBytes = subtitleInfo.sizeBytes;
            }
            const mediaPlaylistPath = join(hlsDir, "media.m3u8");
            const masterPlaylistPath = join(hlsDir, "master.m3u8");
            await this.buildFinalPlaylists(job.id, mediaPlaylistPath, masterPlaylistPath, probe, Boolean(job.source_subtitle_path));
            job = this.store.updateJob(job.id, {
                status: "uploading",
                phase: "uploading",
                last_heartbeat_at: nowIso(),
            });
            await this.throwIfCancelled(job.id);
            await this.r2.clearPrefix(job.r2_prefix);
            await this.uploadOutputs(job, hlsDir);
            const sourceVideoObjectKey = `${job.r2_prefix}/source/video.${job.source_video_ext}`;
            const sourceVideoUrl = this.r2.getPublicUrl(sourceVideoObjectKey);
            const streamObjectKey = `${job.r2_prefix}/hls/master.m3u8`;
            const streamUrl = this.r2.getPublicUrl(streamObjectKey);
            const hlsSize = await getDirectorySize(hlsDir);
            if (job.source_subtitle_path) {
                subtitleObjectKey = `${job.r2_prefix}/source/subtitle.vtt`;
                subtitleUrl = this.r2.getPublicUrl(subtitleObjectKey);
                subtitleFileName = "subtitle.vtt";
                subtitleSizeBytes = (await stat(join(hlsDir, "subtitles", "subtitles.vtt"))).size;
            }
            await this.supabase.syncEpisodeMedia({
                episodeId: job.episode_id,
                channelId: job.channel_id,
                videoUrl: sourceVideoUrl,
                videoObjectKey: sourceVideoObjectKey,
                videoFileName: `video.${job.source_video_ext}`,
                videoMimeType: this.inferVideoMimeType(job.source_video_ext),
                videoSizeBytes: (await stat(job.source_video_path)).size,
                subtitleUrl,
                subtitleObjectKey,
                subtitleFileName,
                subtitleSizeBytes,
                streamUrl,
                streamObjectKey,
                hlsPackageSizeBytes: hlsSize,
                requestedBy: job.requested_by,
                ownerUserId: job.owner_user_id,
                contentId: job.content_id,
                r2Prefix: job.r2_prefix,
                durationSeconds: probe.durationSeconds,
            });
            this.store.addEvent(job.id, "info", "Job completed", {
                streamUrl,
                hlsSize,
            });
            this.store.updateJob(job.id, {
                status: "completed",
                phase: "completed",
                result_video_url: sourceVideoUrl,
                result_subtitle_url: subtitleUrl,
                result_stream_url: streamUrl,
                stream_master_object_key: streamObjectKey,
                source_video_object_key: sourceVideoObjectKey,
                source_subtitle_object_key: subtitleObjectKey,
                result_hls_size_bytes: hlsSize,
                elapsed_seconds: this.computeElapsedSeconds(this.store.getJob(job.id)),
                completed_at: nowIso(),
                last_heartbeat_at: nowIso(),
            });
        }
        catch (error) {
            if (error instanceof CancelledError) {
                this.store.addEvent(job.id, "warn", "Job cancelled");
                this.store.updateJob(job.id, {
                    status: "cancelled",
                    phase: "cancelled",
                    completed_at: nowIso(),
                    elapsed_seconds: this.computeElapsedSeconds(this.store.getJob(job.id)),
                });
            }
            else {
                const reason = error instanceof Error ? error.message : "Unknown media processing error";
                this.store.addEvent(job.id, "error", "Job failed", { reason });
                this.store.updateJob(job.id, {
                    status: "failed",
                    phase: "failed",
                    failure_reason: reason,
                    completed_at: nowIso(),
                    elapsed_seconds: this.computeElapsedSeconds(this.store.getJob(job.id)),
                });
            }
        }
        finally {
            this.activeChild = null;
            this.activeJobId = null;
        }
    }
    async ensureBranches(job, probe) {
        const existingBranches = this.store.getBranches(job.id);
        if (existingBranches.length)
            return existingBranches;
        const branchDuration = 1800;
        const totalBranches = Math.max(1, Math.ceil(probe.durationSeconds / branchDuration));
        const branches = Array.from({ length: totalBranches }, (_, branchIndex) => {
            const startSeconds = branchIndex * branchDuration;
            const endSeconds = Math.min(probe.durationSeconds, startSeconds + branchDuration);
            return {
                job_id: job.id,
                branch_index: branchIndex,
                start_seconds: startSeconds,
                end_seconds: endSeconds,
                target_duration_seconds: endSeconds - startSeconds,
                status: "pending",
                resume_from_seconds: 0,
                duration_completed_seconds: 0,
                segment_count: 0,
                playlist_path: null,
                last_error: null,
                started_at: null,
                completed_at: null,
            };
        });
        this.store.insertBranches(job.id, branches);
        this.store.updateJob(job.id, {
            planned_branch_count: totalBranches,
            current_branch_index: 0,
        });
        this.store.addEvent(job.id, "info", "Branches planned", {
            totalBranches,
            duration: probe.durationSeconds,
        });
        return this.store.getBranches(job.id);
    }
    async processBranches(jobId) {
        const branches = this.store.getBranches(jobId);
        for (const branch of branches) {
            await this.throwIfCancelled(jobId);
            if (branch.status === "completed")
                continue;
            await this.processBranch(jobId, branch);
            const job = this.store.getJob(jobId);
            const completedBranchCount = this.store.getBranches(jobId).filter((item) => item.status === "completed").length;
            const elapsedSeconds = this.computeElapsedSeconds(job);
            const estimatedCompletionAt = completedBranchCount > 0 && job.planned_branch_count > 0
                ? new Date(new Date(job.started_at || job.created_at).getTime() +
                    (elapsedSeconds / completedBranchCount) * job.planned_branch_count * 1000).toISOString()
                : null;
            this.store.updateJob(jobId, {
                completed_branch_count: completedBranchCount,
                current_branch_index: Math.min(branch.branch_index + 1, Math.max(job.planned_branch_count - 1, 0)),
                elapsed_seconds: elapsedSeconds,
                estimated_completion_at: estimatedCompletionAt,
                last_heartbeat_at: nowIso(),
            });
        }
    }
    async processBranch(jobId, branch) {
        const job = this.store.getJob(jobId);
        const workDir = join(config.mediaWorkDir, job.id);
        const branchDir = join(workDir, "hls", "branches", `branch-${String(branch.branch_index).padStart(3, "0")}`);
        const branchPlaylistPath = join(branchDir, "index.m3u8");
        const tempPlaylistPath = join(branchDir, `part-${Date.now()}.m3u8`);
        const segmentPattern = join(branchDir, "seg-%05d.ts");
        await ensureDir(branchDir);
        const existingState = await this.loadBranchProgress(branchDir, branchPlaylistPath);
        if (existingState.completedDuration >= branch.target_duration_seconds - 1) {
            this.store.updateBranch(jobId, branch.branch_index, {
                status: "completed",
                playlist_path: branchPlaylistPath,
                duration_completed_seconds: branch.target_duration_seconds,
                segment_count: existingState.segments.length,
                completed_at: nowIso(),
            });
            return;
        }
        const remainingSeconds = Math.max(branch.target_duration_seconds - existingState.completedDuration, 0.25);
        const startAt = branch.start_seconds + existingState.completedDuration;
        this.store.updateJob(jobId, {
            phase: `branch ${branch.branch_index + 1}/${job.planned_branch_count || 1}`,
            current_branch_index: branch.branch_index,
            last_heartbeat_at: nowIso(),
        });
        this.store.updateBranch(jobId, branch.branch_index, {
            status: "running",
            started_at: branch.started_at || nowIso(),
            resume_from_seconds: startAt,
            duration_completed_seconds: existingState.completedDuration,
            segment_count: existingState.segments.length,
            playlist_path: branchPlaylistPath,
        });
        this.store.addEvent(jobId, "info", "Branch processing started", {
            branchIndex: branch.branch_index,
            startAt,
            remainingSeconds,
        });
        try {
            await this.runFfmpeg(buildBranchFfmpegArgs({
                inputPath: job.source_video_path,
                startSeconds: startAt,
                durationSeconds: remainingSeconds,
                segmentStartNumber: existingState.segments.length,
                playlistPath: tempPlaylistPath,
                segmentPattern,
            }), jobId);
            const newSegments = await parsePlaylistFile(tempPlaylistPath);
            const mergedSegments = [...existingState.segments, ...newSegments];
            await writePlaylist(branchPlaylistPath, mergedSegments);
            await rm(tempPlaylistPath, { force: true });
            this.store.updateBranch(jobId, branch.branch_index, {
                status: "completed",
                duration_completed_seconds: branch.target_duration_seconds,
                segment_count: mergedSegments.length,
                playlist_path: branchPlaylistPath,
                completed_at: nowIso(),
                last_error: null,
            });
            this.store.addEvent(jobId, "info", "Branch completed", {
                branchIndex: branch.branch_index,
                segments: mergedSegments.length,
            });
        }
        catch (error) {
            const reason = error instanceof Error ? error.message : "Branch processing failed";
            this.store.updateBranch(jobId, branch.branch_index, {
                status: this.store.getJob(jobId)?.cancel_requested ? "cancelled" : "failed",
                last_error: reason,
            });
            throw error;
        }
    }
    async loadBranchProgress(branchDir, playlistPath) {
        try {
            const playlistRaw = await readFile(playlistPath, "utf8");
            const parsedSegments = parsePlaylistSegments(playlistRaw);
            const validSegments = [];
            for (const segment of parsedSegments) {
                const fullPath = join(branchDir, segment.uri);
                try {
                    await stat(fullPath);
                    validSegments.push(segment);
                }
                catch {
                    break;
                }
            }
            return {
                segments: validSegments,
                completedDuration: validSegments.reduce((sum, item) => sum + item.duration, 0),
            };
        }
        catch {
            return { segments: [], completedDuration: 0 };
        }
    }
    async buildFinalPlaylists(jobId, mediaPlaylistPath, masterPlaylistPath, probe, hasSubtitle) {
        const branches = this.store.getBranches(jobId);
        const allSegments = [];
        for (const branch of branches) {
            const branchPlaylistPath = branch.playlist_path || join(config.mediaWorkDir, jobId, "hls", "branches", `branch-${String(branch.branch_index).padStart(3, "0")}`, "index.m3u8");
            const segments = await parsePlaylistFile(branchPlaylistPath);
            const prefix = `branches/branch-${String(branch.branch_index).padStart(3, "0")}`;
            allSegments.push(...segments.map((segment) => ({ duration: segment.duration, uri: `${prefix}/${segment.uri}` })));
        }
        await writePlaylist(mediaPlaylistPath, allSegments);
        await writeFile(masterPlaylistPath, buildMasterPlaylist({
            bitrate: probe.bitrate,
            width: probe.width,
            height: probe.height,
            hasSubtitle,
        }), "utf8");
    }
    async prepareSubtitle(job, subtitleDir, durationSeconds) {
        if (!job.source_subtitle_path) {
            throw new Error("Subtitle path is missing");
        }
        const outputPath = join(subtitleDir, "subtitles.vtt");
        await ensureDir(subtitleDir);
        if (job.source_subtitle_ext === "vtt") {
            await copyFile(job.source_subtitle_path, outputPath);
        }
        else {
            await this.runFfmpeg(buildSubtitleConvertArgs(job.source_subtitle_path, outputPath), job.id);
        }
        await createSubtitlePlaylist(subtitleDir, durationSeconds);
        return {
            localPath: outputPath,
            objectKey: `${job.r2_prefix}/source/subtitle.vtt`,
            publicUrl: this.r2.getPublicUrl(`${job.r2_prefix}/source/subtitle.vtt`),
            fileName: "subtitle.vtt",
            sizeBytes: (await stat(outputPath)).size,
        };
    }
    async uploadOutputs(job, hlsDir) {
        const sourceDir = join(config.mediaWorkDir, job.id, "source");
        await removeDirIfExists(sourceDir);
        await ensureDir(sourceDir);
        await copyFile(job.source_video_path, join(sourceDir, `video.${job.source_video_ext}`));
        if (job.source_subtitle_path) {
            await copyFile(join(hlsDir, "subtitles", "subtitles.vtt"), join(sourceDir, "subtitle.vtt"));
        }
        await this.r2.uploadDirectory(sourceDir, `${job.r2_prefix}/source`);
        await this.r2.uploadDirectory(hlsDir, `${job.r2_prefix}/hls`);
    }
    async runFfmpeg(args, jobId) {
        await this.throwIfCancelled(jobId);
        await new Promise((resolve, reject) => {
            const child = spawn(ffmpegBinaryPath, args, {
                stdio: ["ignore", "ignore", "pipe"],
            });
            this.activeChild = child;
            let stderr = "";
            const heartbeatTimer = setInterval(() => {
                const job = this.store.getJob(jobId);
                if (!job)
                    return;
                this.store.updateJob(jobId, {
                    last_heartbeat_at: nowIso(),
                    elapsed_seconds: this.computeElapsedSeconds(job),
                });
            }, 5000);
            child.stderr.on("data", (chunk) => {
                stderr = `${stderr}${chunk.toString()}`.slice(-6000);
            });
            child.on("error", (error) => {
                clearInterval(heartbeatTimer);
                this.activeChild = null;
                reject(error);
            });
            child.on("close", (code, signal) => {
                clearInterval(heartbeatTimer);
                this.activeChild = null;
                if (this.store.getJob(jobId)?.cancel_requested) {
                    reject(new CancelledError(`Cancelled while ffmpeg was running (${signal || code || "unknown"})`));
                    return;
                }
                if (code === 0) {
                    resolve();
                    return;
                }
                reject(new Error(stderr || `ffmpeg exited with code ${code}`));
            });
        });
    }
    computeElapsedSeconds(job) {
        const startAt = job.started_at || job.created_at;
        return Math.max((Date.now() - new Date(startAt).getTime()) / 1000, 0);
    }
    inferVideoMimeType(extension) {
        switch (extension) {
            case "mp4":
                return "video/mp4";
            case "webm":
                return "video/webm";
            case "mkv":
                return "video/x-matroska";
            case "mov":
                return "video/quicktime";
            default:
                return "application/octet-stream";
        }
    }
    async throwIfCancelled(jobId) {
        const job = this.store.getJob(jobId);
        if (job?.cancel_requested) {
            throw new CancelledError(job.cancel_reason || "Cancelled by user");
        }
    }
}
