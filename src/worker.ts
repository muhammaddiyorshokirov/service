import { spawn, type ChildProcess } from "node:child_process";
import { copyFile, readFile, rm, stat, writeFile } from "node:fs/promises";
import { join } from "node:path";
import type { FastifyBaseLogger } from "fastify";
import { config } from "./config.js";
import { MediaStore } from "./db.js";
import {
  buildBranchFfmpegArgs,
  buildMasterPlaylist,
  buildSubtitleConvertArgs,
  createSubtitlePlaylist,
  ffmpegBinaryPath,
  parsePlaylistFile,
  probeMedia,
  writePlaylist,
} from "./hls.js";
import { R2Storage } from "./r2.js";
import { SupabaseBridge } from "./supabase.js";
import type { BranchRow, JobRow, MediaProbe, PlaylistSegment } from "./types.js";
import {
  ensureDir,
  getDirectorySize,
  nowIso,
  parsePlaylistSegments,
  removeDirIfExists,
} from "./utils.js";

class CancelledError extends Error {
  constructor(message = "Job cancelled") {
    super(message);
    this.name = "CancelledError";
  }
}

export class MediaJobWorker {
  private readonly store: MediaStore;
  private readonly supabase: SupabaseBridge;
  private readonly r2: R2Storage;
  private readonly logger: FastifyBaseLogger;
  private ticking = false;
  private activeJobId: string | null = null;
  private activeChild: ChildProcess | null = null;

  constructor(input: {
    store: MediaStore;
    supabase: SupabaseBridge;
    r2: R2Storage;
    logger: FastifyBaseLogger;
  }) {
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

  requestCancel(jobId: string) {
    if (this.activeJobId !== jobId || !this.activeChild) return;
    this.activeChild.kill("SIGTERM");
    setTimeout(() => {
      if (this.activeChild && this.activeJobId === jobId) {
        this.activeChild.kill("SIGKILL");
      }
    }, 5000).unref();
  }

  private async tick() {
    if (this.ticking) return;
    this.ticking = true;

    try {
      const nextJob = this.store.getNextQueuedJob();
      if (!nextJob) return;
      await this.processJob(nextJob.id);
    } catch (error) {
      this.logger.error({ error }, "Media worker tick failed");
    } finally {
      this.ticking = false;
      if (this.store.getNextQueuedJob()) {
        this.schedule();
      }
    }
  }

  private async processJob(jobId: string) {
    let job = this.store.getJob(jobId);
    if (!job) return;

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
      const previousMedia = await this.supabase.getEpisodeMedia(job.episode_id);
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

      let subtitleUrl: string | null = null;
      let subtitleObjectKey: string | null = null;
      let subtitleFileName: string | null = null;
      let subtitleSizeBytes: number | null = null;

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
      await this.cleanupInputArtifacts(job.id);

      job = this.store.updateJob(job.id, {
        status: "uploading",
        phase: "uploading",
        last_heartbeat_at: nowIso(),
      });

      await this.throwIfCancelled(job.id);
      await this.r2.clearPrefix(job.r2_prefix);
      await this.uploadOutputs(job, hlsDir);

      const sourceVideoObjectKey = null;
      const sourceVideoUrl = null;
      const streamObjectKey = `${job.r2_prefix}/hls/master.m3u8`;
      const streamUrl = this.r2.getPublicUrl(streamObjectKey);
      const hlsSize = await getDirectorySize(hlsDir);

      await this.supabase.syncEpisodeMedia({
        episodeId: job.episode_id,
        channelId: job.channel_id,
        videoUrl: sourceVideoUrl,
        videoObjectKey: sourceVideoObjectKey,
        videoFileName: null,
        videoMimeType: null,
        videoSizeBytes: null,
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
        durationSeconds: probe.durationSeconds,
      });
      await this.cleanupObsoleteEpisodeAssets(previousMedia, {
        currentRootPrefix: job.r2_prefix,
        currentStreamObjectKey: streamObjectKey,
        currentSubtitleObjectKey: subtitleObjectKey,
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
        elapsed_seconds: this.computeElapsedSeconds(this.store.getJob(job.id)!),
        completed_at: nowIso(),
        last_heartbeat_at: nowIso(),
      });
    } catch (error) {
      if (error instanceof CancelledError) {
        this.store.addEvent(job.id, "warn", "Job cancelled");
        this.store.updateJob(job.id, {
          status: "cancelled",
          phase: "cancelled",
          completed_at: nowIso(),
          elapsed_seconds: this.computeElapsedSeconds(this.store.getJob(job.id)!),
        });
      } else {
        const reason = error instanceof Error ? error.message : "Unknown media processing error";
        this.store.addEvent(job.id, "error", "Job failed", { reason });
        this.store.updateJob(job.id, {
          status: "failed",
          phase: "failed",
          failure_reason: reason,
          completed_at: nowIso(),
          elapsed_seconds: this.computeElapsedSeconds(this.store.getJob(job.id)!),
        });
      }
    } finally {
      this.activeChild = null;
      this.activeJobId = null;
      try {
        await removeDirIfExists(join(config.mediaWorkDir, job.id));
      } catch (cleanupError) {
        this.logger.warn({ cleanupError, jobId: job.id }, "Failed to cleanup media work directory");
      }
    }
  }

  private async ensureBranches(job: JobRow, probe: MediaProbe) {
    const existingBranches = this.store.getBranches(job.id);
    if (existingBranches.length) return existingBranches;

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
        status: "pending" as const,
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

  private async processBranches(jobId: string) {
    const branches = this.store.getBranches(jobId);
    for (const branch of branches) {
      await this.throwIfCancelled(jobId);
      if (branch.status === "completed") continue;

      await this.processBranch(jobId, branch);
      const job = this.store.getJob(jobId)!;
      const completedBranchCount = this.store.getBranches(jobId).filter((item) => item.status === "completed").length;
      const elapsedSeconds = this.computeElapsedSeconds(job);
      const estimatedCompletionAt =
        completedBranchCount > 0 && job.planned_branch_count > 0
          ? new Date(
              new Date(job.started_at || job.created_at).getTime() +
                (elapsedSeconds / completedBranchCount) * job.planned_branch_count * 1000,
            ).toISOString()
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

  private async processBranch(jobId: string, branch: BranchRow) {
    const job = this.store.getJob(jobId)!;
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
      await this.runFfmpeg(
        buildBranchFfmpegArgs({
          inputPath: job.source_video_path,
          startSeconds: startAt,
          durationSeconds: remainingSeconds,
          segmentStartNumber: existingState.segments.length,
          playlistPath: tempPlaylistPath,
          segmentPattern,
        }),
        jobId,
      );

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
    } catch (error) {
      const reason = error instanceof Error ? error.message : "Branch processing failed";
      this.store.updateBranch(jobId, branch.branch_index, {
        status: this.store.getJob(jobId)?.cancel_requested ? "cancelled" : "failed",
        last_error: reason,
      });
      throw error;
    }
  }

  private async loadBranchProgress(branchDir: string, playlistPath: string) {
    try {
      const playlistRaw = await readFile(playlistPath, "utf8");
      const parsedSegments = parsePlaylistSegments(playlistRaw);
      const validSegments: PlaylistSegment[] = [];

      for (const segment of parsedSegments) {
        const fullPath = join(branchDir, segment.uri);
        try {
          await stat(fullPath);
          validSegments.push(segment);
        } catch {
          break;
        }
      }

      return {
        segments: validSegments,
        completedDuration: validSegments.reduce((sum, item) => sum + item.duration, 0),
      };
    } catch {
      return { segments: [] as PlaylistSegment[], completedDuration: 0 };
    }
  }

  private async buildFinalPlaylists(
    jobId: string,
    mediaPlaylistPath: string,
    masterPlaylistPath: string,
    probe: MediaProbe,
    hasSubtitle: boolean,
  ) {
    const branches = this.store.getBranches(jobId);
    const allSegments: PlaylistSegment[] = [];

    for (const branch of branches) {
      const branchPlaylistPath = branch.playlist_path || join(
        config.mediaWorkDir,
        jobId,
        "hls",
        "branches",
        `branch-${String(branch.branch_index).padStart(3, "0")}`,
        "index.m3u8",
      );
      const segments = await parsePlaylistFile(branchPlaylistPath);
      const prefix = `branches/branch-${String(branch.branch_index).padStart(3, "0")}`;
      allSegments.push(...segments.map((segment) => ({ duration: segment.duration, uri: `${prefix}/${segment.uri}` })));
    }

    await writePlaylist(mediaPlaylistPath, allSegments);
    await writeFile(
      masterPlaylistPath,
      buildMasterPlaylist({
        bitrate: probe.bitrate,
        width: probe.width,
        height: probe.height,
        hasSubtitle,
      }),
      "utf8",
    );
  }

  private async prepareSubtitle(job: JobRow, subtitleDir: string, durationSeconds: number) {
    if (!job.source_subtitle_path) {
      throw new Error("Subtitle path is missing");
    }

    const outputPath = join(subtitleDir, "subtitles.vtt");
    await ensureDir(subtitleDir);

    if (job.source_subtitle_ext === "vtt") {
      await copyFile(job.source_subtitle_path, outputPath);
    } else {
      await this.runFfmpeg(buildSubtitleConvertArgs(job.source_subtitle_path, outputPath), job.id);
    }

    await createSubtitlePlaylist(subtitleDir, durationSeconds);

    return {
      localPath: outputPath,
      objectKey: `${job.r2_prefix}/hls/subtitles/subtitles.vtt`,
      publicUrl: this.r2.getPublicUrl(`${job.r2_prefix}/hls/subtitles/subtitles.vtt`),
      fileName: "subtitles.vtt",
      sizeBytes: (await stat(outputPath)).size,
    };
  }

  private async uploadOutputs(job: JobRow, hlsDir: string) {
    await this.r2.uploadDirectory(hlsDir, `${job.r2_prefix}/hls`);
  }

  private async cleanupInputArtifacts(jobId: string) {
    await removeDirIfExists(join(config.mediaWorkDir, jobId, "input"));
  }

  private async cleanupObsoleteEpisodeAssets(
    previousMedia: { videoUrl: string | null; streamUrl: string | null; subtitleUrl: string | null },
    current: {
      currentRootPrefix: string;
      currentStreamObjectKey: string;
      currentSubtitleObjectKey: string | null;
    },
  ) {
    const protectedKeys = new Set(
      [current.currentStreamObjectKey, current.currentSubtitleObjectKey].filter(
        (value): value is string => Boolean(value),
      ),
    );
    const exactKeys = new Set<string>();
    const prefixes = new Set<string>();

    const collect = (url?: string | null) => {
      const objectKey = this.extractObjectKeyFromUrl(url);
      if (!objectKey) return;
      if (protectedKeys.has(objectKey)) return;
      if (objectKey.startsWith(`${current.currentRootPrefix}/`)) return;

      if (objectKey.toLowerCase().endsWith(".m3u8")) {
        const prefix = this.getParentPrefix(objectKey);
        if (prefix) {
          prefixes.add(prefix);
        }
        return;
      }

      exactKeys.add(objectKey);
    };

    collect(previousMedia.videoUrl);
    collect(previousMedia.streamUrl);
    collect(previousMedia.subtitleUrl);

    for (const key of [...exactKeys]) {
      const coveredByPrefix = [...prefixes].some((prefix) => key.startsWith(`${prefix}/`));
      if (coveredByPrefix) {
        exactKeys.delete(key);
      }
    }

    for (const prefix of prefixes) {
      await this.r2.clearPrefix(prefix);
    }

    if (exactKeys.size) {
      await this.r2.deleteObjects([...exactKeys]);
    }
  }

  private async runFfmpeg(args: string[], jobId: string) {
    await this.throwIfCancelled(jobId);
    await new Promise<void>((resolve, reject) => {
      const child = spawn(ffmpegBinaryPath, args, {
        stdio: ["ignore", "ignore", "pipe"],
      });
      this.activeChild = child;

      let stderr = "";
      const heartbeatTimer = setInterval(() => {
        const job = this.store.getJob(jobId);
        if (!job) return;
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

  private computeElapsedSeconds(job: JobRow) {
    const startAt = job.started_at || job.created_at;
    return Math.max((Date.now() - new Date(startAt).getTime()) / 1000, 0);
  }

  private inferVideoMimeType(extension: string) {
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

  private async throwIfCancelled(jobId: string) {
    const job = this.store.getJob(jobId);
    if (job?.cancel_requested) {
      throw new CancelledError(job.cancel_reason || "Cancelled by user");
    }
  }

  private extractObjectKeyFromUrl(url?: string | null) {
    if (!url) return null;

    try {
      return new URL(url).pathname.replace(/^\/+/, "") || null;
    } catch {
      return url.split("?")[0]?.replace(/^\/+/, "") || null;
    }
  }

  private getParentPrefix(objectKey: string) {
    const parts = objectKey.split("/").filter(Boolean);
    if (parts.length <= 1) return null;
    return parts.slice(0, -1).join("/");
  }
}
