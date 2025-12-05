import { BlobServiceClient } from "@azure/storage-blob";
import { serve } from "@hono/node-server";
import { zValidator } from "@hono/zod-validator";
import { Hono } from "hono";
import { z } from "zod";
import { watermarkImage } from "./lib/processor.js";

const app = new Hono();

const blobService = BlobServiceClient.fromConnectionString(
  process.env.AZURE_STORAGE_CONNECTION_STRING!
);
const containerClient = blobService.getContainerClient("cams"); // make sure this container exists

const newJobSchema = z.object({
  container: z.string().min(1),
  blobName: z.string().min(1),
  type: z.enum(["image", "video"]),
  responseUrl: z.url(),
  watermarkText: z.string().min(1),
});

app.post("/new-item", zValidator("json", newJobSchema), async (c) => {
  const { container, blobName, type, responseUrl, watermarkText } =
    c.req.valid("json");

  // Get input blob
  const containerClient = blobService.getContainerClient(container);
  const inBlob = containerClient.getBlobClient(blobName);
  const download = await inBlob.download();
  const inBuf = Buffer.from(await (await download.blobBody!).arrayBuffer());

  let processedBuffer: Buffer<ArrayBufferLike> | null = null; // Placeholder - replace with actual ffmpeg processing
  if (type === "image") {
    processedBuffer = await watermarkImage(inBuf, watermarkText);
  } else if (type === "video") {
    return c.json({ error: "Video processing not implemented" }, 501);
  }

  if (!processedBuffer) {
    return c.json({ error: "Processing failed" }, 500);
  }

  // After processing, upload back to Blob
  const outName = `processed/${blobName}`;
  const outBlob = containerClient.getBlockBlobClient(outName);
  await outBlob.uploadData(processedBuffer);

  // Notify Discord via responseUrl
  await fetch(responseUrl, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      content: "✅ Job complete!",
      embeds: [{ image: { url: outBlob.url } }],
    }),
  });

  return c.json({ status: "accepted" });
});

serve(
  {
    fetch: app.fetch,
    port: 3000,
  },
  (info) => {
    console.log(`Server is running on http://localhost:${info.port}`);
  }
);
