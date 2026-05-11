import {
    S3Client,
    PutObjectCommand,
    ListObjectsV2Command,
    DeleteObjectsCommand,
} from '@aws-sdk/client-s3';
import fs from 'node:fs';
import crypto from 'node:crypto';
import {config} from '../config.js';
import {logger} from '../util/logger.js';

export class S3Uploader {
    constructor({
        bucket          = config.s3Bucket,
        region          = config.awsRegion,
        accessKeyId     = config.awsAccessKeyId,
        secretAccessKey = config.awsSecretAccessKey,
    } = {}) {
        this.bucket = bucket;
        this.region = region;
        this.accessKeyId = accessKeyId;
        this.secretAccessKey = secretAccessKey;
        this._client = null;
    }

    isEnabled() {
        return Boolean(this.bucket);
    }

    _getClient() {
        if (this._client) return this._client;
        // If both keys are set, pass explicit credentials (matches the env-var
        // names avni-infra already exports). Otherwise pass undefined so the
        // SDK's default credential chain takes over — picks up the EC2 instance
        // role in production, or ~/.aws/credentials locally.
        const credentials = (this.accessKeyId && this.secretAccessKey)
            ? {accessKeyId: this.accessKeyId, secretAccessKey: this.secretAccessKey}
            : undefined;
        this._client = new S3Client({region: this.region, credentials});
        return this._client;
    }

    async uploadFile(localPath, s3Key) {
        const body = fs.readFileSync(localPath);
        const sha256 = crypto.createHash('sha256').update(body).digest('hex');
        const sizeBytes = body.length;

        await this._getClient().send(new PutObjectCommand({
            Bucket: this.bucket,
            Key: s3Key,
            Body: body,
            ContentType: 'application/octet-stream',
        }));

        return {
            s3Key,
            s3Url: `s3://${this.bucket}/${s3Key}`,
            sha256,
            sizeBytes,
        };
    }

    async deletePrefix(prefix) {
        const client = this._getClient();
        let continuationToken;
        let total = 0;
        do {
            const list = await client.send(new ListObjectsV2Command({
                Bucket: this.bucket,
                Prefix: prefix,
                ContinuationToken: continuationToken,
            }));
            const contents = list.Contents ?? [];
            if (contents.length > 0) {
                await client.send(new DeleteObjectsCommand({
                    Bucket: this.bucket,
                    Delete: {Objects: contents.map(o => ({Key: o.Key})), Quiet: true},
                }));
                total += contents.length;
            }
            continuationToken = list.IsTruncated ? list.NextContinuationToken : undefined;
        } while (continuationToken);
        if (total > 0) logger.info({prefix, total}, 's3 deletePrefix');
        return total;
    }
}
