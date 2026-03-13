import { mkdir, readdir, rm, stat } from "node:fs/promises";
import { dirname, extname } from "node:path";
export function nowIso() {
    return new Date().toISOString();
}
export function slugifySegment(value, fallback) {
    const normalized = (value || "")
        .normalize("NFKD")
        .replace(/[^\x00-\x7F]/g, "")
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, "-")
        .replace(/(^-|-$)/g, "");
    return normalized || fallback;
}
export function clampNumber(value, min, max) {
    return Math.min(Math.max(value, min), max);
}
export async function ensureDir(path) {
    await mkdir(path, { recursive: true });
}
export function getFileExtension(fileName) {
    return extname(fileName).replace(".", "").toLowerCase() || "bin";
}
export async function ensureParentDir(path) {
    await ensureDir(dirname(path));
}
export async function removeDirIfExists(path) {
    await rm(path, { recursive: true, force: true });
}
export function parsePlaylistSegments(playlistText) {
    const lines = playlistText.split(/\r?\n/);
    const segments = [];
    for (let index = 0; index < lines.length; index += 1) {
        const line = lines[index]?.trim();
        if (!line?.startsWith("#EXTINF:"))
            continue;
        const durationText = line.replace("#EXTINF:", "").replace(/,$/, "");
        const uri = lines[index + 1]?.trim();
        const duration = Number(durationText);
        if (!uri || Number.isNaN(duration))
            continue;
        segments.push({ duration, uri });
    }
    return segments;
}
export function buildVodPlaylist(segments) {
    const targetDuration = Math.max(1, ...segments.map((segment) => Math.ceil(segment.duration)));
    const lines = [
        "#EXTM3U",
        "#EXT-X-VERSION:3",
        `#EXT-X-TARGETDURATION:${targetDuration}`,
        "#EXT-X-MEDIA-SEQUENCE:0",
    ];
    for (const segment of segments) {
        lines.push(`#EXTINF:${segment.duration.toFixed(6)},`);
        lines.push(segment.uri);
    }
    lines.push("#EXT-X-ENDLIST");
    return lines.join("\n");
}
export function formatDuration(seconds) {
    if (!seconds || seconds <= 0)
        return "0s";
    const total = Math.round(seconds);
    const hours = Math.floor(total / 3600);
    const minutes = Math.floor((total % 3600) / 60);
    const secs = total % 60;
    if (hours > 0)
        return `${hours}h ${minutes}m`;
    if (minutes > 0)
        return `${minutes}m ${secs}s`;
    return `${secs}s`;
}
export async function getDirectorySize(path) {
    let total = 0;
    const entries = await readdir(path, { withFileTypes: true });
    for (const entry of entries) {
        const fullPath = `${path}/${entry.name}`;
        if (entry.isDirectory()) {
            total += await getDirectorySize(fullPath);
            continue;
        }
        const entryStat = await stat(fullPath);
        total += entryStat.size;
    }
    return total;
}
export function inferContentTypeFromKey(key) {
    const ext = getFileExtension(key);
    switch (ext) {
        case "m3u8":
            return "application/vnd.apple.mpegurl";
        case "ts":
            return "video/mp2t";
        case "vtt":
            return "text/vtt; charset=utf-8";
        case "srt":
            return "application/x-subrip";
        case "mp4":
            return "video/mp4";
        case "mkv":
            return "video/x-matroska";
        case "webm":
            return "video/webm";
        case "mov":
            return "video/quicktime";
        case "json":
            return "application/json";
        default:
            return "application/octet-stream";
    }
}
