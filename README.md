# VoiPlay Media Service

Node.js service for copy-only HLS packaging, queue management, and Cloudflare R2 delivery.

## Environment

Set these variables before running:

- `PORT`
- `SUPABASE_URL`
- `SUPABASE_ANON_KEY`
- `SUPABASE_SERVICE_ROLE_KEY`
- `R2_ENDPOINT`
- `R2_ACCESS_KEY_ID`
- `R2_SECRET_ACCESS_KEY`
- `R2_BUCKET_NAME`
- `R2_PUBLIC_URL`
- `MEDIA_DB_PATH`
- `MEDIA_WORKDIR`
- `MAX_UPLOAD_BYTES`
- `ALLOWED_ORIGINS`
- `FFMPEG_PATH` (optional, default: `ffmpeg`)
- `NODE_OPTIONS=--max-old-space-size=384`

## Run

```bash
npm install
npm run dev
```

The service now uses the system `ffmpeg` binary for both metadata probing and media processing.
On Render native environments, `ffmpeg` is available by default, so the default `FFMPEG_PATH=ffmpeg` is sufficient.

## Render

Use these settings in Render for a native Node service:

- Root Directory: `web-service`
- Build Command: `npm run render:build`
- Start Command: `npm start`
- Health Check Path: `/health`

Required environment variables in Render:

- `SUPABASE_URL`
- `SUPABASE_ANON_KEY`
- `SUPABASE_SERVICE_ROLE_KEY`
- `R2_ENDPOINT`
- `R2_ACCESS_KEY_ID`
- `R2_SECRET_ACCESS_KEY`
- `R2_BUCKET_NAME`
- `R2_PUBLIC_URL`

Optional:

- `FFMPEG_PATH` (leave unset on Render unless `ffmpeg` is in a custom path)
- `MEDIA_DB_PATH`
- `MEDIA_WORKDIR`
- `MAX_UPLOAD_BYTES`
- `ALLOWED_ORIGINS`
- `NODE_OPTIONS=--max-old-space-size=384`

## Expected domains

- CMS: `https://admin.voiplay.uz`
- Media service: `https://service.voiplay.uz`
