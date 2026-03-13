import { readFile, readdir } from "node:fs/promises";
import { join, relative } from "node:path";
import { DeleteObjectsCommand, ListObjectsV2Command, PutObjectCommand, S3Client, } from "@aws-sdk/client-s3";
import { ensureDir, inferContentTypeFromKey } from "./utils.js";
export class R2Storage {
    client;
    bucketName;
    publicBaseUrl;
    constructor(input) {
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
    getPublicUrl(objectKey) {
        return `${this.publicBaseUrl}/${objectKey}`;
    }
    async clearPrefix(prefix) {
        let continuationToken;
        do {
            const listResponse = await this.client.send(new ListObjectsV2Command({
                Bucket: this.bucketName,
                Prefix: `${prefix}/`,
                ContinuationToken: continuationToken,
            }));
            const objects = (listResponse.Contents || [])
                .map((item) => item.Key)
                .filter((item) => Boolean(item));
            if (objects.length) {
                await this.client.send(new DeleteObjectsCommand({
                    Bucket: this.bucketName,
                    Delete: {
                        Objects: objects.map((key) => ({ Key: key })),
                        Quiet: true,
                    },
                }));
            }
            continuationToken = listResponse.IsTruncated
                ? listResponse.NextContinuationToken || undefined
                : undefined;
        } while (continuationToken);
    }
    async uploadFile(localPath, objectKey) {
        const body = await readFile(localPath);
        await this.client.send(new PutObjectCommand({
            Bucket: this.bucketName,
            Key: objectKey,
            Body: body,
            ContentType: inferContentTypeFromKey(objectKey),
        }));
    }
    async uploadDirectory(localRoot, remotePrefix) {
        await ensureDir(localRoot);
        const files = await this.collectFiles(localRoot);
        for (const file of files) {
            const relativePath = relative(localRoot, file).replace(/\\/g, "/");
            const objectKey = `${remotePrefix}/${relativePath}`;
            await this.uploadFile(file, objectKey);
        }
    }
    async collectFiles(rootPath) {
        const entries = await readdir(rootPath, { withFileTypes: true });
        const results = [];
        for (const entry of entries) {
            const fullPath = join(rootPath, entry.name);
            if (entry.isDirectory()) {
                results.push(...(await this.collectFiles(fullPath)));
            }
            else {
                results.push(fullPath);
            }
        }
        return results;
    }
}
