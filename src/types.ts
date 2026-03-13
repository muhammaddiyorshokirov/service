export type JobStatus =
  | "queued"
  | "running"
  | "uploading"
  | "completed"
  | "failed"
  | "cancelling"
  | "cancelled";

export type BranchStatus =
  | "pending"
  | "running"
  | "completed"
  | "failed"
  | "cancelled";

export interface AuthContext {
  accessToken: string;
  userId: string;
  fullName: string | null;
  roles: string[];
  isAdmin: boolean;
}

export interface EpisodeContext {
  episodeId: string;
  contentId: string;
  channelId: string;
  episodeNumber: number;
  contentTitle: string | null;
  channelName: string | null;
  episodeTitle: string | null;
  ownerUserId: string;
}

export interface MediaProbe {
  durationSeconds: number;
  bitrate: number | null;
  width: number | null;
  height: number | null;
  videoCodec: string | null;
  audioCodec: string | null;
  raw: unknown;
}

export interface PlaylistSegment {
  duration: number;
  uri: string;
}

export interface JobRow {
  id: string;
  episode_id: string;
  content_id: string;
  channel_id: string;
  owner_user_id: string;
  requested_by: string;
  requested_by_name: string | null;
  episode_number: number;
  channel_name: string | null;
  content_title: string | null;
  episode_title: string | null;
  source_video_path: string;
  source_video_ext: string;
  source_video_original_name: string;
  source_subtitle_path: string | null;
  source_subtitle_ext: string | null;
  source_subtitle_original_name: string | null;
  r2_prefix: string;
  source_video_object_key: string | null;
  source_subtitle_object_key: string | null;
  stream_master_object_key: string | null;
  status: JobStatus;
  phase: string;
  cancel_requested: number;
  cancel_reason: string | null;
  failure_reason: string | null;
  current_branch_index: number | null;
  planned_branch_count: number;
  completed_branch_count: number;
  duration_seconds: number | null;
  video_bitrate: number | null;
  video_width: number | null;
  video_height: number | null;
  video_codec: string | null;
  audio_codec: string | null;
  elapsed_seconds: number | null;
  estimated_completion_at: string | null;
  last_heartbeat_at: string | null;
  started_at: string | null;
  completed_at: string | null;
  result_video_url: string | null;
  result_subtitle_url: string | null;
  result_stream_url: string | null;
  result_hls_size_bytes: number | null;
  ffprobe_json: string | null;
  created_at: string;
  updated_at: string;
}

export interface BranchRow {
  id: string;
  job_id: string;
  branch_index: number;
  start_seconds: number;
  end_seconds: number;
  target_duration_seconds: number;
  status: BranchStatus;
  resume_from_seconds: number;
  duration_completed_seconds: number;
  segment_count: number;
  playlist_path: string | null;
  last_error: string | null;
  started_at: string | null;
  completed_at: string | null;
  updated_at: string;
}

export interface JobEventRow {
  id: number;
  job_id: string;
  level: string;
  message: string;
  data_json: string | null;
  created_at: string;
}

export interface JobWithRelations {
  job: JobRow;
  branches: BranchRow[];
  events: JobEventRow[];
}
