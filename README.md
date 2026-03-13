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
- `NODE_OPTIONS=--max-old-space-size=384`

## Run

```bash
npm install
npm run dev
```

The service bundles `ffmpeg` and `ffprobe` through installer packages, so it does not depend on system binaries.

## Expected domains

- CMS: `https://admin.voiplay.uz`
- Media service: `https://service.voiplay.uz`
