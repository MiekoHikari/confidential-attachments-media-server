import { spawn } from "child_process";
import { writeFile, unlink, mkdtemp, rmdir } from "fs/promises";
import { tmpdir } from "os";
import { join } from "path";
import { loadImage, createCanvas } from "@napi-rs/canvas";
import { log } from "../lib/utils/logger.js";
import { watermarkGenerator } from "./watermark.service.js";

export class MediaService {
  async watermarkImage(buffer: Buffer, text: string): Promise<Buffer> {
    const image = await loadImage(buffer);
    const canvas = createCanvas(image.width, image.height);
    const ctx = canvas.getContext("2d");

    ctx.drawImage(image, 0, 0);

    const watermarkBuffer = watermarkGenerator.generate(
      image.width,
      image.height,
      text
    );
    const watermarkOverlay = await loadImage(watermarkBuffer);
    ctx.drawImage(watermarkOverlay, 0, 0);

    return canvas.toBuffer("image/png");
  }

  async getVideoDimensions(
    urlOrPath: string
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
        urlOrPath,
      ]);

      let stdout = "";
      ffprobe.stdout.on("data", (d) => (stdout += d));
      ffprobe.on("close", (code) => {
        if (code !== 0) return reject(new Error("FFprobe failed"));
        const result = JSON.parse(stdout);
        resolve({
          width: result.streams[0].width,
          height: result.streams[0].height,
        });
      });
    });
  }

  /**
   * Streams video processing: Input URL -> FFmpeg -> Output Stream (Azure)
   */
  async watermarkVideoStream(
    inputUrl: string,
    watermarkText: string,
    uploadStreamFn: (stdout: NodeJS.ReadableStream) => Promise<void>
  ): Promise<void> {
    const tempDir = await mkdtemp(join(tmpdir(), "watermark-"));
    const overlayPath = join(tempDir, "overlay.png");

    try {
      const { width, height } = await this.getVideoDimensions(inputUrl);
      const overlayBuffer = watermarkGenerator.generate(
        width,
        height,
        watermarkText
      );
      await writeFile(overlayPath, overlayBuffer);

      log("FFMPEG", "Starting stream transcoding...");

      const ffmpeg = spawn("ffmpeg", [
        "-i",
        inputUrl,
        "-i",
        overlayPath,
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
        "pipe:1",
      ]);

      // Hook up the FFmpeg stdout to the provided upload function
      await Promise.all([
        uploadStreamFn(ffmpeg.stdout),
        new Promise<void>((resolve, reject) => {
          ffmpeg.on("close", (code) =>
            code === 0 ? resolve() : reject(new Error(`FFmpeg exited ${code}`))
          );
          ffmpeg.on("error", reject);

          ffmpeg.stderr.on("data", (data) => {
            log("FFMPEG", `Stderr: ${data}`);
          });
        }),
      ]);
    } finally {
      await unlink(overlayPath).catch(() => {});
      await rmdir(tempDir).catch(() => {});
    }
  }
}

export const mediaService = new MediaService();
