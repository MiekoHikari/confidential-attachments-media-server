import "./lib/setup.js";
import { BlobClient, BlobServiceClient } from "@azure/storage-blob";
import { serve } from "@hono/node-server";
import { zValidator } from "@hono/zod-validator";
import { Hono } from "hono";
import { logger } from "hono/logger";
import { watermarkImage, watermarkVideoToAzure } from "./lib/processor.js";
import { newJobSchema } from "./lib/types.js";

const app = new Hono();

// Request logging middleware
app.use("*", logger());

// Custom error logging
app.onError((err, c) => {
  console.error(
    `[ERROR] ${new Date().toISOString()} - ${c.req.method} ${c.req.path}`
  );
  console.error(`[ERROR] Message: ${err.message}`);
  console.error(`[ERROR] Stack:`, err.stack);
  return c.json({ error: "Internal Server Error", message: err.message }, 500);
});

console.log(
  `[INIT] ${new Date().toISOString()} - Initializing Azure Blob Service Client...`
);
const blobService = BlobServiceClient.fromConnectionString(
  process.env.AZURE_STORAGE_CONNECTION_STRING!
);
console.log(
  `[INIT] ${new Date().toISOString()} - Azure Blob Service Client initialized`
);

async function downloadBlobToBuffer(blobClient: BlobClient): Promise<Buffer> {
  const download = await blobClient.download();

  const chunks: Buffer[] = [];
  for await (const chunk of download.readableStreamBody!) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }

  return Buffer.concat(chunks);
}

app.post("/new-item", zValidator("json", newJobSchema), async (c) => {
  const startTime = Date.now();
  const requestId = crypto.randomUUID();

  const {
    container,
    jobId,
    type,
    responseUrl,
    watermarkText,
    interaction,
    filename,
  } = c.req.valid("json");

  console.log(
    `[REQUEST:${requestId}] ${new Date().toISOString()} - New job received`
  );
  console.log(`[REQUEST:${requestId}] Job ID: ${jobId}`);
  console.log(`[REQUEST:${requestId}] Container: ${container}`);
  console.log(`[REQUEST:${requestId}] Type: ${type}`);
  console.log(`[REQUEST:${requestId}] Filename: ${filename}`);
  console.log(`[REQUEST:${requestId}] Response URL: ${responseUrl}`);
  console.log(
    `[REQUEST:${requestId}] Watermark text length: ${watermarkText.length} chars`
  );

  console.log(`[REQUEST:${requestId}] Fetching blob from container...`);
  const containerClient = blobService.getContainerClient(container);
  const inBlob = containerClient.getBlobClient(jobId);

  console.log(`[REQUEST:${requestId}] Downloading blob: ${jobId}`);
  const downloadStart = Date.now();
  console.log(
    `[REQUEST:${requestId}] Blob download initiated, reading stream...`
  );

  const inBuf = await downloadBlobToBuffer(inBlob);

  const downloadTime = Date.now() - downloadStart;
  console.log(`[REQUEST:${requestId}] Blob downloaded successfully`);
  console.log(
    `[REQUEST:${requestId}] Download size: ${(inBuf.length / 1024).toFixed(
      2
    )} KB`
  );
  console.log(`[REQUEST:${requestId}] Download time: ${downloadTime}ms`);

  let processedBuffer: Buffer<ArrayBufferLike> | null = null;
  console.log(`[REQUEST:${requestId}] Starting ${type} processing...`);
  const processStart = Date.now();

  if (type === "image") {
    console.log(`[REQUEST:${requestId}] Applying watermark to image...`);
    processedBuffer = await watermarkImage(inBuf, watermarkText);

    c.json({ status: "accepted" });

    const processTime = Date.now() - processStart;
    console.log(`[REQUEST:${requestId}] Image watermarking complete`);

    console.log(
      `[REQUEST:${requestId}] Output size: ${(
        processedBuffer.length / 1024
      ).toFixed(2)} KB`
    );

    console.log(`[REQUEST:${requestId}] Processing time: ${processTime}ms`);
  } else if (type === "video") {
    console.log(
      `[REQUEST:${requestId}] Applying watermark to video and streaming to Azure...`
    );

    c.json({ status: "accepted" });

    // Stream watermarked video directly to Azure (more memory efficient)
    const blobUrl = await watermarkVideoToAzure(
      inBuf,
      watermarkText,
      container,
      jobId
    );

    const processTime = Date.now() - processStart;
    console.log(`[REQUEST:${requestId}] Video watermarking + upload complete`);
    console.log(`[REQUEST:${requestId}] Blob URL: ${blobUrl}`);
    console.log(`[REQUEST:${requestId}] Processing time: ${processTime}ms`);

    // Send callback directly since video is already uploaded
    console.log(`[REQUEST:${requestId}] Sending callback to response URL...`);
    const callbackStart = Date.now();
    const callbackResponse = await fetch(responseUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        jobId,
        interaction,
        filename,
      }),
    });
    const callbackTime = Date.now() - callbackStart;
    console.log(
      `[REQUEST:${requestId}] Callback response status: ${callbackResponse.status}`
    );
    console.log(`[REQUEST:${requestId}] Callback time: ${callbackTime}ms`);

    const totalTime = Date.now() - startTime;
    console.log(`[REQUEST:${requestId}] Job completed successfully`);
    console.log(`[REQUEST:${requestId}] Total processing time: ${totalTime}ms`);
    console.log(
      `[REQUEST:${requestId}] ----------------------------------------`
    );
  }

  if (!processedBuffer) {
    console.error(
      `[REQUEST:${requestId}] Processing failed - no output buffer`
    );
    return c.json({ error: "Processing failed" }, 500);
  }

  // After processing, upload back to Blob
  console.log(
    `[REQUEST:${requestId}] Uploading processed file back to blob...`
  );

  const uploadStart = Date.now();
  const outName = `${jobId}`;
  const outBlob = containerClient.getBlockBlobClient(outName);
  await outBlob.uploadData(processedBuffer);
  const uploadTime = Date.now() - uploadStart;
  console.log(`[REQUEST:${requestId}] Upload complete`);
  console.log(`[REQUEST:${requestId}] Upload time: ${uploadTime}ms`);

  console.log(`[REQUEST:${requestId}] Sending callback to response URL...`);
  const callbackStart = Date.now();

  const callbackResponse = await fetch(responseUrl, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      jobId,
      interaction,
      filename,
    }),
  });

  const callbackTime = Date.now() - callbackStart;
  console.log(
    `[REQUEST:${requestId}] Callback response status: ${callbackResponse.status}`
  );
  console.log(`[REQUEST:${requestId}] Callback time: ${callbackTime}ms`);

  const totalTime = Date.now() - startTime;
  console.log(`[REQUEST:${requestId}] Job completed successfully`);
  console.log(`[REQUEST:${requestId}] Total processing time: ${totalTime}ms`);
  console.log(
    `[REQUEST:${requestId}] ----------------------------------------`
  );

  return;
});

const port = Number(process.env.PORT) || 3000;

console.log(`[INIT] ${new Date().toISOString()} - Starting server...`);
serve(
  {
    fetch: app.fetch,
    port,
  },
  (info) => {
    console.log(
      `[INIT] ${new Date().toISOString()} - Server is running on http://localhost:${
        info.port
      }`
    );
    console.log(`[INIT] Environment: ${process.env.NODE_ENV}`);
    console.log(`[INIT] Ready to accept requests`);
    console.log(`[INIT] ========================================`);
  }
);
