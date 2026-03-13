import { spawn } from "node:child_process";
import { writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { buildVodPlaylist, ensureDir, parsePlaylistSegments } from "./utils.js";
export const ffmpegBinaryPath = process.env.FFMPEG_PATH || "ffmpeg";
export async function probeMedia(inputPath) {
    const raw = await runProcess(ffmpegBinaryPath, [
        "-hide_banner",
        "-i",
        inputPath,
        "-t",
        "0",
        "-f",
        "null",
        "-",
    ]);
    const parsed = parseFfmpegProbe(raw.stderr);
    const durationSeconds = parsed.durationSeconds;
    if (!Number.isFinite(durationSeconds) || durationSeconds <= 0) {
        throw new Error("Video duration could not be determined");
    }
    return {
        durationSeconds,
        bitrate: parsed.bitrate,
        width: parsed.width,
        height: parsed.height,
        videoCodec: parsed.videoCodec,
        audioCodec: parsed.audioCodec,
        raw: {
            command: ffmpegBinaryPath,
            stderr: raw.stderr,
            parsed,
        },
    };
}
export function buildBranchFfmpegArgs(options) {
    return [
        "-hide_banner",
        "-loglevel",
        "warning",
        "-ss",
        options.startSeconds.toString(),
        "-i",
        options.inputPath,
        "-t",
        options.durationSeconds.toString(),
        "-map",
        "0:v:0",
        "-map",
        "0:a?",
        "-c",
        "copy",
        "-f",
        "hls",
        "-hls_time",
        "10",
        "-hls_segment_type",
        "mpegts",
        "-hls_flags",
        "temp_file",
        "-hls_list_size",
        "0",
        "-start_number",
        options.segmentStartNumber.toString(),
        "-hls_segment_filename",
        options.segmentPattern,
        options.playlistPath,
    ];
}
export function buildSubtitleConvertArgs(inputPath, outputPath) {
    return ["-hide_banner", "-loglevel", "warning", "-i", inputPath, "-y", outputPath];
}
export async function createSubtitlePlaylist(outputDir, durationSeconds) {
    await ensureDir(outputDir);
    const playlistPath = join(outputDir, "subtitles.m3u8");
    const text = [
        "#EXTM3U",
        "#EXT-X-VERSION:3",
        `#EXT-X-TARGETDURATION:${Math.max(1, Math.ceil(durationSeconds))}`,
        "#EXT-X-MEDIA-SEQUENCE:0",
        `#EXTINF:${durationSeconds.toFixed(6)},`,
        "subtitles.vtt",
        "#EXT-X-ENDLIST",
    ].join("\n");
    await writeFile(playlistPath, text, "utf8");
    return playlistPath;
}
export function buildMasterPlaylist(options) {
    const lines = ["#EXTM3U", "#EXT-X-VERSION:3"];
    const bandwidth = options.bitrate && options.bitrate > 0 ? options.bitrate : 2_500_000;
    const attributes = [`BANDWIDTH=${bandwidth}`];
    if (options.width && options.height) {
        attributes.push(`RESOLUTION=${options.width}x${options.height}`);
    }
    if (options.hasSubtitle) {
        lines.push('#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID="subs",NAME="Default",DEFAULT=YES,AUTOSELECT=YES,LANGUAGE="uz",URI="subtitles/subtitles.m3u8"');
        attributes.push('SUBTITLES="subs"');
    }
    lines.push(`#EXT-X-STREAM-INF:${attributes.join(",")}`);
    lines.push("media.m3u8");
    return lines.join("\n");
}
export async function parsePlaylistFile(playlistPath) {
    const { readFile } = await import("node:fs/promises");
    const raw = await readFile(playlistPath, "utf8");
    return parsePlaylistSegments(raw);
}
export async function writePlaylist(playlistPath, segments) {
    await ensureDir(dirname(playlistPath));
    await writeFile(playlistPath, buildVodPlaylist(segments), "utf8");
}
function runProcess(command, args) {
    return new Promise((resolve, reject) => {
        const child = spawn(command, args, {
            stdio: ["ignore", "pipe", "pipe"],
        });
        let stdout = "";
        let stderr = "";
        child.stdout.on("data", (chunk) => {
            stdout += chunk.toString();
        });
        child.stderr.on("data", (chunk) => {
            stderr += chunk.toString();
        });
        child.on("error", reject);
        child.on("close", (code) => {
            if (code === 0) {
                resolve({ stdout, stderr });
                return;
            }
            reject(new Error(stderr || `Process exited with code ${code}`));
        });
    });
}
function parseFfmpegProbe(stderr) {
    const durationMatch = stderr.match(/Duration:\s*(\d+):(\d+):(\d+(?:\.\d+)?)/);
    const durationSeconds = durationMatch
        ? Number(durationMatch[1]) * 3600 + Number(durationMatch[2]) * 60 + Number(durationMatch[3])
        : NaN;
    const bitrateMatch = stderr.match(/bitrate:\s*([0-9.]+)\s*([kmg])?b\/s/i);
    const bitrate = bitrateMatch ? normalizeBitrate(bitrateMatch[1], bitrateMatch[2]) : null;
    const lines = stderr
        .split(/\r?\n/)
        .map((line) => line.trim())
        .filter(Boolean);
    const videoLine = lines.find((line) => line.includes("Video:")) || null;
    const audioLine = lines.find((line) => line.includes("Audio:")) || null;
    const resolutionMatch = videoLine?.match(/(?:^|,\s*)(\d{2,5})x(\d{2,5})(?:[\s,\[]|$)/);
    return {
        durationSeconds,
        bitrate,
        width: resolutionMatch ? Number(resolutionMatch[1]) : null,
        height: resolutionMatch ? Number(resolutionMatch[2]) : null,
        videoCodec: parseCodecName(videoLine, "Video:"),
        audioCodec: parseCodecName(audioLine, "Audio:"),
    };
}
function parseCodecName(line, marker) {
    if (!line)
        return null;
    const afterMarker = line.split(marker)[1]?.trim();
    if (!afterMarker)
        return null;
    const codecMatch = afterMarker.match(/^([a-z0-9._-]+)/i);
    return codecMatch ? codecMatch[1].toLowerCase() : null;
}
function normalizeBitrate(rawValue, unit) {
    const value = Number(rawValue);
    if (!Number.isFinite(value) || value <= 0)
        return null;
    switch ((unit || "k").toLowerCase()) {
        case "g":
            return Math.round(value * 1_000_000_000);
        case "m":
            return Math.round(value * 1_000_000);
        case "k":
            return Math.round(value * 1_000);
        default:
            return Math.round(value);
    }
}
