import { BlobServiceClient, BlobSASPermissions } from "@azure/storage-blob";
import { envParseString } from "@skyra/env-utilities";
import { log } from "../lib/utils/logger.js";

export class StorageService {
  private blobService: BlobServiceClient;

  constructor() {
    const connectionString = envParseString("AZURE_STORAGE_CONNECTION_STRING");
    this.blobService = BlobServiceClient.fromConnectionString(connectionString);
  }

  async downloadBuffer(container: string, blobName: string): Promise<Buffer> {
    const client = this.blobService
      .getContainerClient(container)
      .getBlobClient(blobName);
    const download = await client.download();
    const chunks: Buffer[] = [];
    for await (const chunk of download.readableStreamBody!) {
      chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    }
    return Buffer.concat(chunks);
  }

  async uploadBuffer(
    container: string,
    blobName: string,
    buffer: Buffer,
    contentType?: string
  ): Promise<void> {
    const client = this.blobService
      .getContainerClient(container)
      .getBlockBlobClient(blobName);
    await client.uploadData(buffer, {
      blobHTTPHeaders: contentType
        ? { blobContentType: contentType }
        : undefined,
    });
  }

  async generateSasUrl(container: string, blobName: string): Promise<string> {
    const client = this.blobService
      .getContainerClient(container)
      .getBlobClient(blobName);
    return await client.generateSasUrl({
      permissions: BlobSASPermissions.parse("r"),
      expiresOn: new Date(Date.now() + 60 * 60 * 1000),
    });
  }

  /**
   * Returns a BlockBlobClient for streaming uploads
   */
  getUploadClient(container: string, blobName: string) {
    return this.blobService
      .getContainerClient(container)
      .getBlockBlobClient(blobName);
  }
}

export const storageService = new StorageService();
