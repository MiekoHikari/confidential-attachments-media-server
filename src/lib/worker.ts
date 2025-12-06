import { Worker, Job } from "bullmq";
import { redisConnection } from "./mq.js";
import { log } from "./utils/logger.js";
import { storageService } from "../services/storage.service.js";
import { mediaService } from "../services/media.service.js";
import type { newJobSchema } from "./types.js";
import type z from "zod";
import type Stream from "stream";

type JobData = z.infer<typeof newJobSchema>;

async function processJob(job: Job<JobData>): Promise<void> {
  const {
    container,
    jobId,
    type,
    responseUrl,
    watermarkText,
    interaction,
    filename,
  } = job.data;
  const start = Date.now();

  log("WORKER", `[JOB:${jobId}] Starting ${type} processing`);
  await job.updateProgress(10);

  if (type === "image") {
    // 1. Download
    const inputBuffer = await storageService.downloadBuffer(container, jobId);
    await job.updateProgress(30);

    // 2. Process
    const outputBuffer = await mediaService.watermarkImage(
      inputBuffer,
      watermarkText
    );
    await job.updateProgress(60);

    // 3. Upload
    await storageService.uploadBuffer(
      container,
      jobId,
      outputBuffer,
      "image/png"
    );
    await job.updateProgress(90);
  } else if (type === "video") {
    // 1. Prepare Access
    const sasUrl = await storageService.generateSasUrl(container, jobId);

    // 2. Prepare Upload Target
    const uploadClient = storageService.getUploadClient(container, jobId);

    // 3. Stream Process (Pipeline: Azure -> FFmpeg -> Azure)
    await mediaService.watermarkVideoStream(
      sasUrl,
      watermarkText,
      async (ffmpegStream) => {
        // This callback bridges the Media Service output to the Storage Service input
        await uploadClient.uploadStream(
          ffmpegStream as Stream.Readable,
          4 * 1024 * 1024,
          4,
          {
            blobHTTPHeaders: { blobContentType: "video/mp4" },
          }
        );
      }
    );

    await job.updateProgress(90);
  }

  // 4. Callback
  log("WORKER", `[JOB:${jobId}] Sending callback to: ${responseUrl}`);
  try {
    const response = await fetch(responseUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ jobId, interaction, filename }),
    });
    log("WORKER", `[JOB:${jobId}] Callback response: ${response.status}`);
  } catch (err) {
    log("WORKER", `[JOB:${jobId}] Callback failed: ${err}`);
    throw err;
  }

  log("WORKER", `[JOB:${jobId}] Completed in ${Date.now() - start}ms`);
}

// Worker Configuration
const worker = new Worker<JobData>("watermark", processJob, {
  connection: redisConnection,
  concurrency: 1,
  lockDuration: 60000,
  lockRenewTime: 30000,
  removeOnComplete: { count: 100 },
  removeOnFail: { count: 3 }, // Immediately remove failed jobs from queue
});

worker.on("completed", (job) => log("WORKER", `Job ${job.id} done`));
worker.on("failed", async (job, err) => {
  log("WORKER", `Job ${job?.id} failed: ${err.message}`);
});
worker.on("error", (err) => log("WORKER", `System error: ${err.message}`));
