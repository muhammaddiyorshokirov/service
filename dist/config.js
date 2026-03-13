import { resolve } from "node:path";
function readEnv(name, fallback) {
    const value = process.env[name] ?? fallback;
    if (!value) {
        throw new Error(`Missing required environment variable: ${name}`);
    }
    return value;
}
function readInt(name, fallback) {
    const raw = process.env[name];
    if (!raw)
        return fallback;
    const parsed = Number(raw);
    if (!Number.isFinite(parsed) || parsed <= 0) {
        throw new Error(`Invalid numeric environment variable: ${name}`);
    }
    return Math.trunc(parsed);
}
export const config = {
    port: readInt("PORT", 8788),
    supabaseUrl: readEnv("SUPABASE_URL"),
    supabaseAnonKey: readEnv("SUPABASE_ANON_KEY"),
    supabaseServiceRoleKey: readEnv("SUPABASE_SERVICE_ROLE_KEY"),
    r2Endpoint: readEnv("R2_ENDPOINT"),
    r2AccessKeyId: readEnv("R2_ACCESS_KEY_ID"),
    r2SecretAccessKey: readEnv("R2_SECRET_ACCESS_KEY"),
    r2BucketName: readEnv("R2_BUCKET_NAME"),
    r2PublicUrl: readEnv("R2_PUBLIC_URL"),
    mediaDbPath: resolve(process.env.MEDIA_DB_PATH || "./data/media-service.sqlite"),
    mediaWorkDir: resolve(process.env.MEDIA_WORKDIR || "./data/workdir"),
    maxUploadBytes: readInt("MAX_UPLOAD_BYTES", 1024 * 1024 * 1024),
    allowedOrigins: (process.env.ALLOWED_ORIGINS ||
        "https://admin.voiplay.uz,https://service.voiplay.uz,http://localhost:5173,http://127.0.0.1:5173")
        .split(",")
        .map((value) => value.trim())
        .filter(Boolean),
};
