import { createClient, type SupabaseClient } from "@supabase/supabase-js";
import type { AuthContext, EpisodeContext } from "./types.js";

interface SyncEpisodeMediaInput {
  episodeId: string;
  channelId: string;
  videoUrl: string;
  videoObjectKey: string;
  videoFileName: string;
  videoMimeType: string;
  videoSizeBytes: number;
  subtitleUrl: string | null;
  subtitleObjectKey: string | null;
  subtitleFileName: string | null;
  subtitleSizeBytes: number | null;
  streamUrl: string;
  streamObjectKey: string;
  hlsPackageSizeBytes: number;
  requestedBy: string;
  ownerUserId: string;
  contentId: string;
  r2Prefix: string;
  durationSeconds: number;
}

export class SupabaseBridge {
  readonly authClient: SupabaseClient;
  readonly serviceClient: SupabaseClient;

  constructor(url: string, anonKey: string, serviceRoleKey: string) {
    this.authClient = createClient(url, anonKey);
    this.serviceClient = createClient(url, serviceRoleKey);
  }

  async authenticate(accessToken: string): Promise<AuthContext> {
    const {
      data: { user },
      error: authError,
    } = await this.authClient.auth.getUser(accessToken);

    if (authError || !user) {
      throw new Error("Unauthorized");
    }

    const [rolesRes, profileRes] = await Promise.all([
      this.serviceClient.from("user_roles").select("role").eq("user_id", user.id),
      this.serviceClient.from("profiles").select("full_name").eq("id", user.id).maybeSingle(),
    ]);

    if (rolesRes.error) throw rolesRes.error;
    if (profileRes.error) throw profileRes.error;

    const roles = (rolesRes.data || []).map((row) => String(row.role));
    return {
      accessToken,
      userId: user.id,
      fullName: profileRes.data?.full_name || user.user_metadata?.full_name || null,
      roles,
      isAdmin: roles.includes("admin"),
    };
  }

  async getEpisodeContext(episodeId: string): Promise<EpisodeContext> {
    const { data: episode, error: episodeError } = await this.serviceClient
      .from("episodes")
      .select("id, content_id, channel_id, episode_number, title")
      .eq("id", episodeId)
      .maybeSingle();

    if (episodeError) throw episodeError;
    if (!episode?.channel_id || !episode.content_id) {
      throw new Error("Episode not found or channel is missing");
    }

    const [contentRes, channelRes] = await Promise.all([
      this.serviceClient.from("contents").select("id, title").eq("id", episode.content_id).maybeSingle(),
      this.serviceClient
        .from("content_maker_channels")
        .select("id, channel_name, owner_id")
        .eq("id", episode.channel_id)
        .maybeSingle(),
    ]);

    if (contentRes.error) throw contentRes.error;
    if (channelRes.error) throw channelRes.error;
    if (!channelRes.data?.owner_id) {
      throw new Error("Channel owner not found");
    }

    return {
      episodeId: episode.id,
      contentId: episode.content_id,
      channelId: episode.channel_id,
      episodeNumber: Number(episode.episode_number),
      contentTitle: contentRes.data?.title || null,
      channelName: channelRes.data?.channel_name || null,
      episodeTitle: episode.title || null,
      ownerUserId: channelRes.data.owner_id,
    };
  }

  assertEpisodeAccess(auth: AuthContext, episode: EpisodeContext) {
    if (auth.isAdmin) return;
    if (episode.ownerUserId !== auth.userId) {
      throw new Error("Forbidden");
    }
  }

  async syncEpisodeMedia(input: SyncEpisodeMediaInput) {
    const deleteRes = await this.serviceClient
      .from("storage_assets")
      .delete()
      .like("object_key", `${input.r2Prefix}/%`);

    if (deleteRes.error) throw deleteRes.error;

    const rows: Array<Record<string, unknown>> = [
      {
        bucket_name: "default",
        object_key: input.videoObjectKey,
        public_url: input.videoUrl,
        file_name: input.videoFileName,
        file_extension: input.videoFileName.split(".").pop()?.toLowerCase() || null,
        mime_type: input.videoMimeType,
        folder: input.videoObjectKey.split("/").slice(0, -1).join("/"),
        asset_kind: "video",
        size_bytes: input.videoSizeBytes,
        owner_user_id: input.ownerUserId,
        uploaded_by: input.requestedBy,
        content_maker_channel_id: input.channelId,
        content_id: input.contentId,
        episode_id: input.episodeId,
        source_table: "episodes",
        source_column: "video_url",
        metadata: { upload_source: "media-service", logical_asset: false },
      },
      {
        bucket_name: "default",
        object_key: input.streamObjectKey,
        public_url: input.streamUrl,
        file_name: "master.m3u8",
        file_extension: "m3u8",
        mime_type: "application/vnd.apple.mpegurl",
        folder: input.streamObjectKey.split("/").slice(0, -1).join("/"),
        asset_kind: "video",
        size_bytes: input.hlsPackageSizeBytes,
        owner_user_id: input.ownerUserId,
        uploaded_by: input.requestedBy,
        content_maker_channel_id: input.channelId,
        content_id: input.contentId,
        episode_id: input.episodeId,
        source_table: "episodes",
        source_column: "stream_url",
        metadata: { upload_source: "media-service", hls_package: true, logical_asset: true },
      },
    ];

    if (input.subtitleUrl && input.subtitleObjectKey && input.subtitleFileName) {
      rows.push({
        bucket_name: "default",
        object_key: input.subtitleObjectKey,
        public_url: input.subtitleUrl,
        file_name: input.subtitleFileName,
        file_extension: input.subtitleFileName.split(".").pop()?.toLowerCase() || null,
        mime_type: "text/vtt",
        folder: input.subtitleObjectKey.split("/").slice(0, -1).join("/"),
        asset_kind: "subtitle",
        size_bytes: input.subtitleSizeBytes,
        owner_user_id: input.ownerUserId,
        uploaded_by: input.requestedBy,
        content_maker_channel_id: input.channelId,
        content_id: input.contentId,
        episode_id: input.episodeId,
        source_table: "episodes",
        source_column: "subtitle_url",
        metadata: { upload_source: "media-service", logical_asset: false },
      });
    }

    const upsertRes = await this.serviceClient
      .from("storage_assets")
      .upsert(rows, { onConflict: "bucket_name,object_key" });

    if (upsertRes.error) throw upsertRes.error;

    const updateEpisodeRes = await this.serviceClient
      .from("episodes")
      .update({
        video_url: input.videoUrl,
        subtitle_url: input.subtitleUrl,
        stream_url: input.streamUrl,
        duration_seconds: Math.round(input.durationSeconds),
      })
      .eq("id", input.episodeId);

    if (updateEpisodeRes.error) throw updateEpisodeRes.error;

    const recalcRes = await this.serviceClient.rpc("recalculate_channel_storage_usage", {
      _channel_id: input.channelId,
    });

    if (recalcRes.error) throw recalcRes.error;
  }
}
