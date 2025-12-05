import { createCanvas, loadImage } from "@napi-rs/canvas";
import { spawn } from "child_process";
import { writeFile, unlink, mkdtemp } from "fs/promises";
import { tmpdir } from "os";
import { join } from "path";
import {
  BlobServiceClient,
  BlobClient,
  BlobSASPermissions,
} from "@azure/storage-blob";
import { envParseString } from "@skyra/env-utilities";
import { Worker, Job } from "bullmq";
import { redisConnection } from "./mq.js";
import type z from "zod";
import type { newJobSchema } from "./types.js";

// Type for job data based on the schema
type JobData = z.infer<typeof newJobSchema>;

function log(message: string) {
  console.log(`[PROCESSOR] ${new Date().toISOString()} - ${message}`);
}

function createWatermarkBuffer(
  width: number,
  height: number,
  watermark: string
): Buffer {
  const canvas = createCanvas(width, height);
  const ctx = canvas.getContext("2d");

  // Dynamic font size based on image dimensions (scales with image size)
  const baseFontSize = Math.max(16, Math.min(width, height) / 35);
  const lineHeight = baseFontSize * 1.2;

  ctx.font = `bold ${baseFontSize}px sans-serif`;
  ctx.textBaseline = "middle";

  // Rotate context for diagonal watermarks
  ctx.translate(width / 2, height / 2);
  ctx.rotate(-Math.PI / 4);
  ctx.translate(-width / 2, -height / 2);

  const diagonal = Math.sqrt(width * width + height * height);
  // Dynamic step size based on image dimensions
  const stepX = Math.max(200, width / 4);
  const stepY = Math.max(100, height / 6);

  const lines = watermark.split("\n");

  // Tiled watermark pattern with improved visibility
  for (let y = -diagonal; y < diagonal; y += stepY) {
    for (let x = -diagonal; x < diagonal; x += stepX) {
      lines.forEach((line, i) => {
        const drawY = y + i * lineHeight;
        // Stronger outline for better visibility on all backgrounds
        ctx.strokeStyle = "rgba(0, 0, 0, 0.4)";
        ctx.lineWidth = 4;
        ctx.strokeText(line, x, drawY);
        // More visible fill
        ctx.fillStyle = "rgba(255, 255, 255, 0.5)";
        ctx.fillText(line, x, drawY);
      });
    }
  }

  ctx.resetTransform();

  // Reset font for prominent watermarks (slightly larger)
  const prominentFontSize = baseFontSize * 1.2;
  ctx.font = `bold ${prominentFontSize}px sans-serif`;
  const prominentLineHeight = prominentFontSize * 1.2;

  const textWidth = Math.max(
    ...lines.map((line) => ctx.measureText(line).width)
  );
  const textHeight = lines.length * prominentLineHeight;
  const padding = 30;

  // Divide the image into a 3x3 grid and place watermarks in different zones
  // This ensures they are always far apart from each other
  const zoneWidth = (width - textWidth - 2 * padding) / 3;
  const zoneHeight = (height - textHeight - 2 * padding) / 3;

  // Define 9 zones and pick 3 that are well-separated
  const zoneConfigs = [
    { zoneX: 0, zoneY: 0 }, // top-left
    { zoneX: 1, zoneY: 0 }, // top-center
    { zoneX: 2, zoneY: 0 }, // top-right
    { zoneX: 0, zoneY: 1 }, // middle-left
    { zoneX: 1, zoneY: 1 }, // center
    { zoneX: 2, zoneY: 1 }, // middle-right
    { zoneX: 0, zoneY: 2 }, // bottom-left
    { zoneX: 1, zoneY: 2 }, // bottom-center
    { zoneX: 2, zoneY: 2 }, // bottom-right
  ];

  // Select 3 well-separated zones (corners + center, or diagonal pattern)
  const separatedZonePatterns = [
    [0, 4, 8], // diagonal: top-left, center, bottom-right
    [2, 4, 6], // anti-diagonal: top-right, center, bottom-left
    [0, 5, 7], // top-left, middle-right, bottom-center
    [1, 3, 8], // top-center, middle-left, bottom-right
    [2, 3, 7], // top-right, middle-left, bottom-center
  ];

  // Randomly select one of the patterns
  const selectedPattern =
    separatedZonePatterns[
      Math.floor(Math.random() * separatedZonePatterns.length)
    ];

  // Ring/donut watermark settings
  const ringRadius = Math.max(40, Math.min(width, height) / 12);
  const ringThickness = ringRadius * 0.35;
  const innerRadius = ringRadius - ringThickness;

  // Smaller font for text inside the ring
  const ringFontSize = Math.max(10, ringRadius / 3.5);
  ctx.font = `bold ${ringFontSize}px sans-serif`;
  ctx.textAlign = "center";
  ctx.textBaseline = "middle";

  for (const zoneIndex of selectedPattern) {
    const zone = zoneConfigs[zoneIndex];

    // Random position within the selected zone (account for ring size)
    const centerX =
      padding +
      ringRadius +
      zone.zoneX * zoneWidth +
      Math.random() * Math.max(zoneWidth - ringRadius * 2, 10);
    const centerY =
      padding +
      ringRadius +
      zone.zoneY * zoneHeight +
      Math.random() * Math.max(zoneHeight - ringRadius * 2, 10);

    // Draw the ring/donut shape
    ctx.beginPath();
    ctx.arc(centerX, centerY, ringRadius, 0, Math.PI * 2); // Outer circle
    ctx.arc(centerX, centerY, innerRadius, 0, Math.PI * 2, true); // Inner circle (counter-clockwise to cut out)
    ctx.closePath();

    // Semi-transparent ring fill
    ctx.fillStyle = "rgba(0, 255, 255, 0.45)";
    ctx.fill();

    // Ring border for visibility on all backgrounds
    ctx.strokeStyle = "rgba(0, 0, 0, 0.5)";
    ctx.lineWidth = 2;
    ctx.stroke();

    // Draw text inside the ring (centered)
    const ringLineHeight = ringFontSize * 1.15;
    const totalTextHeight = lines.length * ringLineHeight;
    const startY = centerY - totalTextHeight / 2 + ringLineHeight / 2;

    lines.forEach((line, j) => {
      const drawY = startY + j * ringLineHeight;
      // Text outline
      ctx.strokeStyle = "rgba(0, 0, 0, 0.6)";
      ctx.lineWidth = 2.5;
      ctx.strokeText(line, centerX, drawY);
      // Text fill
      ctx.fillStyle = "rgba(255, 255, 255, 0.9)";
      ctx.fillText(line, centerX, drawY);
    });
  }

  return canvas.toBuffer("image/png");
}

export async function watermarkImage(
  buffer: Buffer<ArrayBufferLike>,
  watermark: string
): Promise<Buffer> {
  log(
    `Starting watermark process for buffer of ${(buffer.length / 1024).toFixed(
      2
    )} KB`
  );

  log("Loading source image...");
  const loadStart = Date.now();
  const image = await loadImage(buffer);
  log(`Source image loaded in ${Date.now() - loadStart}ms`);
  log(`Image dimensions: ${image.width}x${image.height}`);

  log("Creating canvas...");
  const canvas = createCanvas(image.width, image.height);
  const ctx = canvas.getContext("2d");

  log("Drawing source image to canvas...");
  ctx.drawImage(image, 0, 0);

  log("Generating watermark overlay...");
  const watermarkStart = Date.now();
  const watermarkBuffer = createWatermarkBuffer(
    image.width,
    image.height,
    watermark
  );
  log(
    `Watermark buffer created in ${Date.now() - watermarkStart}ms (${(
      watermarkBuffer.length / 1024
    ).toFixed(2)} KB)`
  );

  log("Loading watermark as image...");
  const watermarkOverlay = await loadImage(watermarkBuffer);
  log("Compositing watermark onto image...");
  ctx.drawImage(watermarkOverlay, 0, 0);

  log("Encoding final image to PNG buffer...");
  const encodeStart = Date.now();
  const outputBuffer = canvas.toBuffer("image/png");
  log(`Image encoded in ${Date.now() - encodeStart}ms`);
  log(`Output buffer size: ${(outputBuffer.length / 1024).toFixed(2)} KB`);
  log("Watermark process complete");

  return outputBuffer;
}

/**
 * Get video dimensions using FFprobe
 */
async function getVideoDimensions(
  videoPath: string
): Promise<{ width: number; height: number }> {
  return new Promise((resolve, reject) => {
    const ffprobe = spawn("ffprobe", [
      "-v",
      "error",
      "-select_streams",
      "v:0",
      "-show_entries",
      "stream=width,height",
      "-of",
      "json",
      videoPath,
    ]);

    let stdout = "";
    let stderr = "";

    ffprobe.stdout.on("data", (data) => {
      stdout += data.toString();
    });

    ffprobe.stderr.on("data", (data) => {
      stderr += data.toString();
    });

    ffprobe.on("close", (code) => {
      if (code !== 0) {
        reject(new Error(`FFprobe failed: ${stderr}`));
        return;
      }

      try {
        const result = JSON.parse(stdout);
        const stream = result.streams[0];
        resolve({ width: stream.width, height: stream.height });
      } catch (e) {
        reject(new Error(`Failed to parse FFprobe output: ${e}`));
      }
    });
  });
}

/**
 * Get video dimensions from a URL using FFprobe (no download required)
 */
async function getVideoDimensionsFromUrl(
  videoUrl: string
): Promise<{ width: number; height: number }> {
  return new Promise((resolve, reject) => {
    const ffprobe = spawn("ffprobe", [
      "-v",
      "error",
      "-select_streams",
      "v:0",
      "-show_entries",
      "stream=width,height",
      "-of",
      "json",
      videoUrl, // FFprobe can read directly from URLs
    ]);

    let stdout = "";
    let stderr = "";

    ffprobe.stdout.on("data", (data) => {
      stdout += data.toString();
    });

    ffprobe.stderr.on("data", (data) => {
      stderr += data.toString();
    });

    ffprobe.on("close", (code) => {
      if (code !== 0) {
        reject(new Error(`FFprobe failed: ${stderr}`));
        return;
      }

      try {
        const result = JSON.parse(stdout);
        const stream = result.streams[0];
        resolve({ width: stream.width, height: stream.height });
      } catch (e) {
        reject(new Error(`Failed to parse FFprobe output: ${e}`));
      }
    });
  });
}

/**
 * Watermark a video file efficiently by generating a single overlay frame
 * and compositing it onto every frame using FFmpeg
 */
export async function watermarkVideo(
  buffer: Buffer<ArrayBuffer>,
  watermark: string
): Promise<Buffer> {
  log(
    `Starting video watermark process for buffer of ${(
      buffer.length /
      1024 /
      1024
    ).toFixed(2)} MB`
  );

  // Create temp directory for intermediate files
  const tempDir = await mkdtemp(join(tmpdir(), "watermark-"));
  const inputPath = join(tempDir, "input.mp4");
  const overlayPath = join(tempDir, "overlay.png");
  const outputPath = join(tempDir, "output.mp4");

  try {
    // Write input video to temp file
    log("Writing input video to temp file...");
    await writeFile(inputPath, buffer);

    // Get video dimensions using FFprobe
    log("Probing video dimensions...");
    const { width, height } = await getVideoDimensions(inputPath);
    log(`Video dimensions: ${width}x${height}`);

    // Generate watermark overlay (single frame, transparent PNG)
    log("Generating watermark overlay...");
    const overlayStart = Date.now();
    const overlayBuffer = createWatermarkBuffer(width, height, watermark);
    await writeFile(overlayPath, overlayBuffer);
    log(
      `Watermark overlay created in ${Date.now() - overlayStart}ms (${(
        overlayBuffer.length / 1024
      ).toFixed(2)} KB)`
    );

    // Use FFmpeg to overlay watermark onto video
    log("Starting FFmpeg overlay process...");
    const ffmpegStart = Date.now();

    await new Promise<void>((resolve, reject) => {
      const ffmpeg = spawn("ffmpeg", [
        "-y", // Overwrite output
        "-i",
        inputPath, // Input video
        "-i",
        overlayPath, // Watermark overlay
        "-filter_complex",
        "[0:v][1:v]overlay=0:0:format=auto", // Overlay at 0,0
        "-c:v",
        "libx264", // H.264 codec
        "-preset",
        "fast", // Fast encoding (balance speed/quality)
        "-crf",
        "23", // Quality (lower = better, 18-28 is good range)
        "-c:a",
        "copy", // Copy audio without re-encoding
        "-movflags",
        "+faststart", // Optimize for web streaming
        outputPath,
      ]);

      let stderr = "";

      ffmpeg.stderr.on("data", (data) => {
        stderr += data.toString();
      });

      ffmpeg.on("close", (code) => {
        if (code !== 0) {
          log(`FFmpeg stderr: ${stderr}`);
          reject(new Error(`FFmpeg failed with code ${code}`));
          return;
        }
        resolve();
      });

      ffmpeg.on("error", (err) => {
        reject(new Error(`FFmpeg spawn error: ${err.message}`));
      });
    });

    log(`FFmpeg completed in ${Date.now() - ffmpegStart}ms`);

    // Read output video
    log("Reading output video...");
    const { readFile } = await import("fs/promises");
    const outputBuffer = await readFile(outputPath);
    log(
      `Output video size: ${(outputBuffer.length / 1024 / 1024).toFixed(2)} MB`
    );

    log("Video watermark process complete");
    return outputBuffer;
  } finally {
    // Cleanup temp files
    log("Cleaning up temp files...");
    await Promise.allSettled([
      unlink(inputPath),
      unlink(overlayPath),
      unlink(outputPath),
      import("fs/promises").then((fs) => fs.rmdir(tempDir)),
    ]);
  }
}

/**
 * Watermark a video and stream directly to Azure Blob Storage
 * More memory efficient for large videos - doesn't buffer the entire output
 *
 * @param inputBuffer - The input video buffer
 * @param watermark - The watermark text
 * @param containerName - Azure blob container name
 * @param blobName - The blob name (path) in the container
 * @returns The blob URL for the uploaded video
 */
export async function watermarkVideoToAzure(
  inputBuffer: Buffer<ArrayBufferLike>,
  watermark: string,
  containerName: string,
  blobName: string
): Promise<string> {
  log(
    `Starting video watermark + Azure upload for buffer of ${(
      inputBuffer.length /
      1024 /
      1024
    ).toFixed(2)} MB`
  );

  const connectionString = envParseString("AZURE_STORAGE_CONNECTION_STRING");
  const blobServiceClient =
    BlobServiceClient.fromConnectionString(connectionString);
  const containerClient = blobServiceClient.getContainerClient(containerName);
  const blockBlobClient = containerClient.getBlockBlobClient(blobName);

  // Create temp directory for intermediate files
  const tempDir = await mkdtemp(join(tmpdir(), "watermark-"));
  const inputPath = join(tempDir, "input.mp4");
  const overlayPath = join(tempDir, "overlay.png");

  try {
    // Write input video to temp file
    log("Writing input video to temp file...");
    await writeFile(inputPath, inputBuffer);

    // Get video dimensions using FFprobe
    log("Probing video dimensions...");
    const { width, height } = await getVideoDimensions(inputPath);
    log(`Video dimensions: ${width}x${height}`);

    // Generate watermark overlay (single frame, transparent PNG)
    log("Generating watermark overlay...");
    const overlayStart = Date.now();
    const overlayBuffer = createWatermarkBuffer(width, height, watermark);
    await writeFile(overlayPath, overlayBuffer);
    log(
      `Watermark overlay created in ${Date.now() - overlayStart}ms (${(
        overlayBuffer.length / 1024
      ).toFixed(2)} KB)`
    );

    // Use FFmpeg to overlay watermark and output to stdout, stream to Azure
    log("Starting FFmpeg overlay + Azure stream...");
    const ffmpegStart = Date.now();

    await new Promise<void>((resolve, reject) => {
      let ffmpegExitCode: number | null = null;
      let ffmpegDone = false;
      let uploadDone = false;
      let uploadError: Error | null = null;
      let stderr = "";

      const checkCompletion = () => {
        // Only resolve/reject when BOTH FFmpeg has exited AND upload is complete
        if (ffmpegDone && uploadDone) {
          if (ffmpegExitCode !== 0) {
            log(`FFmpeg stderr: ${stderr}`);
            reject(new Error(`FFmpeg failed with code ${ffmpegExitCode}`));
          } else if (uploadError) {
            reject(uploadError);
          } else {
            resolve();
          }
        }
      };

      const ffmpeg = spawn("ffmpeg", [
        "-i",
        inputPath, // Input video
        "-i",
        overlayPath, // Watermark overlay
        "-filter_complex",
        "[0:v][1:v]overlay=0:0:format=auto", // Overlay at 0,0
        "-c:v",
        "libx264", // H.264 codec
        "-preset",
        "fast", // Fast encoding
        "-crf",
        "23", // Quality
        "-c:a",
        "copy", // Copy audio without re-encoding
        "-movflags",
        "frag_keyframe+empty_moov+default_base_moof", // Enable streaming output (fragmented MP4) with proper base offsets
        "-f",
        "mp4", // Output format
        "pipe:1", // Output to stdout
      ]);

      ffmpeg.stderr.on("data", (data) => {
        stderr += data.toString();
      });

      // FFmpeg stdout is already a Node.js Readable stream - use it directly
      blockBlobClient
        .uploadStream(ffmpeg.stdout, 4 * 1024 * 1024, 4, {
          blobHTTPHeaders: {
            blobContentType: "video/mp4",
          },
        })
        .then(() => {
          log(`Upload to Azure completed`);
          uploadDone = true;
          checkCompletion();
        })
        .catch((err) => {
          uploadError = new Error(`Azure upload failed: ${err.message}`);
          uploadDone = true;
          ffmpeg.kill();
          checkCompletion();
        });

      ffmpeg.on("close", (code) => {
        ffmpegExitCode = code;
        ffmpegDone = true;
        checkCompletion();
      });

      ffmpeg.on("error", (err) => {
        reject(new Error(`FFmpeg spawn error: ${err.message}`));
      });
    });

    log(`FFmpeg + Azure upload completed in ${Date.now() - ffmpegStart}ms`);
    log(`Video uploaded to: ${blockBlobClient.url}`);

    return blockBlobClient.url;
  } finally {
    // Cleanup temp files
    log("Cleaning up temp files...");
    await Promise.allSettled([
      unlink(inputPath),
      unlink(overlayPath),
      import("fs/promises").then((fs) => fs.rmdir(tempDir)),
    ]);
  }
}

/**
 * Watermark a video by streaming directly from Azure Blob → FFmpeg → Azure Blob
 * Most memory efficient - never buffers the entire video in memory
 *
 * @param inputBlobUrl - The SAS URL or public URL of the input video blob
 * @param watermark - The watermark text
 * @param containerName - Azure blob container name for output
 * @param blobName - The blob name (path) for the output
 * @returns The blob URL for the uploaded video
 */
export async function watermarkVideoStreamToAzure(
  inputBlobUrl: string,
  watermark: string,
  containerName: string,
  blobName: string
): Promise<string> {
  log(`Starting video stream watermark: Azure → FFmpeg → Azure`);
  log(`Input URL: ${inputBlobUrl.substring(0, 50)}...`);

  const connectionString = envParseString("AZURE_STORAGE_CONNECTION_STRING");
  const blobServiceClient =
    BlobServiceClient.fromConnectionString(connectionString);
  const containerClient = blobServiceClient.getContainerClient(containerName);
  const blockBlobClient = containerClient.getBlockBlobClient(blobName);

  // Create temp directory for overlay only (no video files stored)
  const tempDir = await mkdtemp(join(tmpdir(), "watermark-"));
  const overlayPath = join(tempDir, "overlay.png");

  try {
    // Get video dimensions directly from URL (no download)
    log("Probing video dimensions from URL...");
    const { width, height } = await getVideoDimensionsFromUrl(inputBlobUrl);
    log(`Video dimensions: ${width}x${height}`);

    // Generate watermark overlay (single frame, transparent PNG)
    log("Generating watermark overlay...");
    const overlayStart = Date.now();
    const overlayBuffer = createWatermarkBuffer(width, height, watermark);
    await writeFile(overlayPath, overlayBuffer);
    log(
      `Watermark overlay created in ${Date.now() - overlayStart}ms (${(
        overlayBuffer.length / 1024
      ).toFixed(2)} KB)`
    );

    // Use FFmpeg to stream from URL, overlay watermark, and output to stdout
    log("Starting FFmpeg stream: URL → overlay → Azure...");
    const ffmpegStart = Date.now();

    await new Promise<void>((resolve, reject) => {
      let ffmpegExitCode: number | null = null;
      let ffmpegDone = false;
      let uploadDone = false;
      let uploadError: Error | null = null;
      let stderr = "";

      const checkCompletion = () => {
        if (ffmpegDone && uploadDone) {
          if (ffmpegExitCode !== 0) {
            log(`FFmpeg stderr: ${stderr}`);
            reject(new Error(`FFmpeg failed with code ${ffmpegExitCode}`));
          } else if (uploadError) {
            reject(uploadError);
          } else {
            resolve();
          }
        }
      };

      const ffmpeg = spawn("ffmpeg", [
        "-i",
        inputBlobUrl, // Input directly from URL (Azure streams to FFmpeg)
        "-i",
        overlayPath, // Watermark overlay (local file)
        "-filter_complex",
        "[0:v][1:v]overlay=0:0:format=auto",
        "-c:v",
        "libx264",
        "-preset",
        "fast",
        "-crf",
        "23",
        "-c:a",
        "copy",
        "-movflags",
        "frag_keyframe+empty_moov+default_base_moof",
        "-f",
        "mp4",
        "pipe:1", // Output to stdout (Azure streams from FFmpeg)
      ]);

      ffmpeg.stderr.on("data", (data) => {
        stderr += data.toString();
      });

      // Stream FFmpeg stdout directly to Azure
      blockBlobClient
        .uploadStream(ffmpeg.stdout, 4 * 1024 * 1024, 4, {
          blobHTTPHeaders: {
            blobContentType: "video/mp4",
          },
        })
        .then(() => {
          log(`Upload to Azure completed`);
          uploadDone = true;
          checkCompletion();
        })
        .catch((err) => {
          uploadError = new Error(`Azure upload failed: ${err.message}`);
          uploadDone = true;
          ffmpeg.kill();
          checkCompletion();
        });

      ffmpeg.on("close", (code) => {
        ffmpegExitCode = code;
        ffmpegDone = true;
        checkCompletion();
      });

      ffmpeg.on("error", (err) => {
        reject(new Error(`FFmpeg spawn error: ${err.message}`));
      });
    });

    log(`FFmpeg stream completed in ${Date.now() - ffmpegStart}ms`);
    log(`Video uploaded to: ${blockBlobClient.url}`);

    return blockBlobClient.url;
  } finally {
    // Cleanup temp files (only overlay)
    log("Cleaning up temp files...");
    await Promise.allSettled([
      unlink(overlayPath),
      import("fs/promises").then((fs) => fs.rmdir(tempDir)),
    ]);
  }
}

// ============================================================
// BullMQ Worker Implementation
// ============================================================

const connectionString = envParseString("AZURE_STORAGE_CONNECTION_STRING");
const blobService = BlobServiceClient.fromConnectionString(connectionString);

async function downloadBlobToBuffer(blobClient: BlobClient): Promise<Buffer> {
  const download = await blobClient.download();

  const chunks: Buffer[] = [];
  for await (const chunk of download.readableStreamBody!) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }

  return Buffer.concat(chunks);
}

async function processJob(job: Job<JobData>): Promise<void> {
  const startTime = Date.now();
  const {
    container,
    jobId,
    type,
    responseUrl,
    watermarkText,
    interaction,
    filename,
  } = job.data;

  log(`[JOB:${jobId}] Starting ${type} processing...`);
  await job.updateProgress(10);

  const containerClient = blobService.getContainerClient(container);

  if (type === "image") {
    // For images: download → process → upload
    log(`[JOB:${jobId}] Downloading image blob from container: ${container}`);
    const inBlob = containerClient.getBlobClient(jobId);
    const inBuf = await downloadBlobToBuffer(inBlob);
    log(`[JOB:${jobId}] Downloaded ${(inBuf.length / 1024).toFixed(2)} KB`);
    await job.updateProgress(25);

    // Process image
    log(`[JOB:${jobId}] Applying watermark to image...`);
    const processedBuffer = await watermarkImage(inBuf, watermarkText);
    await job.updateProgress(50);

    // Upload processed image back to Azure
    log(`[JOB:${jobId}] Uploading processed image to Azure...`);
    const outBlob = containerClient.getBlockBlobClient(jobId);
    await outBlob.uploadData(processedBuffer);
    await job.updateProgress(75);
  } else if (type === "video") {
    // For videos: stream directly Azure → FFmpeg → Azure (no memory buffering)
    log(`[JOB:${jobId}] Streaming video: Azure → FFmpeg → Azure...`);

    // Generate a SAS URL for FFmpeg to read from
    const inBlob = containerClient.getBlobClient(jobId);
    const sasUrl = await inBlob.generateSasUrl({
      permissions: BlobSASPermissions.parse("r"),
      expiresOn: new Date(Date.now() + 60 * 60 * 1000), // 1 hour expiry
    });

    // Delete existing blob before streaming new one
    await job.updateProgress(20);

    // Stream directly: Azure blob URL → FFmpeg → Azure blob
    await watermarkVideoStreamToAzure(sasUrl, watermarkText, container, jobId);
    await job.updateProgress(75);
  } else {
    throw new Error(`Unknown job type: ${type}`);
  }

  // Send callback to response URL
  log(`[JOB:${jobId}] Sending callback to response URL...`);
  const callbackResponse = await fetch(responseUrl, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      jobId,
      interaction,
      filename,
    }),
  });

  if (!callbackResponse.ok) {
    throw new Error(`Callback failed with status ${callbackResponse.status}`);
  }

  await job.updateProgress(100);

  const totalTime = Date.now() - startTime;
  log(`[JOB:${jobId}] Job completed successfully in ${totalTime}ms`);
}

// Create and start the worker
const worker = new Worker<JobData>("watermark", processJob, {
  connection: redisConnection,
  concurrency: 1, // FFmpeg is CPU-bound
  lockDuration: 60000, // 60s (match your longest video)
  lockRenewTime: 30000, // Renew every 30s
  stalledInterval: 30000, // Check stalls less often
  maxStalledCount: 1, // Fail after 1 stall
  removeOnComplete: { count: 100 }, // Clean immediately
  removeOnFail: { count: 3 },
});

worker.on("completed", (job) => {
  log(
    `[WORKER] Job ${job.id} completed successfully | Attempts: ${job.attemptsMade}`
  );
});

worker.on("failed", (job, err) => {
  log(
    `[WORKER] Job ${job?.id} failed | Attempts: ${job?.attemptsMade}/${job?.opts.attempts} | Reason: ${err.message}`
  );
  if (job?.failedReason) {
    log(`[WORKER] Failed reason: ${job.failedReason}`);
  }
});

worker.on("error", (err) => {
  log(`[WORKER] Worker error: ${err.message}`);
});

log("[WORKER] Watermark worker started and listening for jobs...");
