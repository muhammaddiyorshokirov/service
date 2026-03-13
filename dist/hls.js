import { spawn } from "node:child_process";
import { writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import ffmpegInstaller from "@ffmpeg-installer/ffmpeg";
import ffprobeInstaller from "@ffprobe-installer/ffprobe";
import { buildVodPlaylist, ensureDir, parsePlaylistSegments } from "./utils.js";
export const ffmpegBinaryPath = ffmpegInstaller.path;
export const ffprobeBinaryPath = ffprobeInstaller.path;
export async function probeMedia(inputPath) {
    const raw = await runProcess(ffprobeBinaryPath, [
        "-v",
        "error",
        "-print_format",
        "json",
        "-show_format",
        "-show_streams",
        inputPath,
    ]);
    const parsed = JSON.parse(raw.stdout);
    const videoStream = Array.isArray(parsed.streams)
        ? parsed.streams.find((stream) => stream.codec_type === "video")
        : null;
    const audioStream = Array.isArray(parsed.streams)
        ? parsed.streams.find((stream) => stream.codec_type === "audio")
        : null;
    const durationRaw = parsed.format?.duration ?? videoStream?.duration ?? audioStream?.duration;
    const durationSeconds = Number(durationRaw);
    if (!Number.isFinite(durationSeconds) || durationSeconds <= 0) {
        throw new Error("Video duration could not be determined");
    }
    const bitrate = Number(parsed.format?.bit_rate);
    const width = Number(videoStream?.width);
    const height = Number(videoStream?.height);
    return {
        durationSeconds,
        bitrate: Number.isFinite(bitrate) ? Math.round(bitrate) : null,
        width: Number.isFinite(width) ? width : null,
        height: Number.isFinite(height) ? height : null,
        videoCodec: typeof videoStream?.codec_name === "string" ? videoStream.codec_name : null,
        audioCodec: typeof audioStream?.codec_name === "string" ? audioStream.codec_name : null,
        raw: parsed,
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
