import { readFile, readdir } from "node:fs/promises";
import { join, relative } from "node:path";
import {
  DeleteObjectsCommand,
  ListObjectsV2Command,
  PutObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";
import { ensureDir, inferContentTypeFromKey } from "./utils.js";

export class R2Storage {
  readonly client: S3Client;
  readonly bucketName: string;
  readonly publicBaseUrl: string;

  constructor(input: {
    endpoint: string;
    accessKeyId: string;
    secretAccessKey: string;
    bucketName: string;
    publicBaseUrl: string;
  }) {
    this.bucketName = input.bucketName;
    this.publicBaseUrl = input.publicBaseUrl.replace(/\/+$/, "");
    this.client = new S3Client({
      region: "auto",
      endpoint: input.endpoint,
      forcePathStyle: true,
      credentials: {
        accessKeyId: input.accessKeyId,
        secretAccessKey: input.secretAccessKey,
      },
    });
  }

  getPublicUrl(objectKey: string) {
    return `${this.publicBaseUrl}/${objectKey}`;
  }

  async clearPrefix(prefix: string) {
    let continuationToken: string | undefined;

    do {
      const listResponse = await this.client.send(
        new ListObjectsV2Command({
          Bucket: this.bucketName,
          Prefix: `${prefix}/`,
          ContinuationToken: continuationToken,
        }),
      );

      const objects = (listResponse.Contents || [])
        .map((item) => item.Key)
        .filter((item): item is string => Boolean(item));

      if (objects.length) {
        await this.client.send(
          new DeleteObjectsCommand({
            Bucket: this.bucketName,
            Delete: {
              Objects: objects.map((key) => ({ Key: key })),
              Quiet: true,
            },
          }),
        );
      }

      continuationToken = listResponse.IsTruncated
        ? listResponse.NextContinuationToken || undefined
        : undefined;
    } while (continuationToken);
  }

  async deleteObjects(objectKeys: string[]) {
    const keys = [...new Set(objectKeys.filter(Boolean))];
    if (!keys.length) return;

    for (let index = 0; index < keys.length; index += 1000) {
      const batch = keys.slice(index, index + 1000);
      await this.client.send(
        new DeleteObjectsCommand({
          Bucket: this.bucketName,
          Delete: {
            Objects: batch.map((key) => ({ Key: key })),
            Quiet: true,
          },
        }),
      );
    }
  }

  async uploadFile(localPath: string, objectKey: string) {
    const body = await readFile(localPath);
    await this.client.send(
      new PutObjectCommand({
        Bucket: this.bucketName,
        Key: objectKey,
        Body: body,
        ContentType: inferContentTypeFromKey(objectKey),
      }),
    );
  }

  async uploadDirectory(localRoot: string, remotePrefix: string) {
    await ensureDir(localRoot);
    const files = await this.collectFiles(localRoot);
    for (const file of files) {
      const relativePath = relative(localRoot, file).replace(/\\/g, "/");
      const objectKey = `${remotePrefix}/${relativePath}`;
      await this.uploadFile(file, objectKey);
    }
  }

  private async collectFiles(rootPath: string): Promise<string[]> {
    const entries = await readdir(rootPath, { withFileTypes: true });
    const results: string[] = [];

    for (const entry of entries) {
      const fullPath = join(rootPath, entry.name);
      if (entry.isDirectory()) {
        results.push(...(await this.collectFiles(fullPath)));
      } else {
        results.push(fullPath);
      }
    }

    return results;
  }
}
