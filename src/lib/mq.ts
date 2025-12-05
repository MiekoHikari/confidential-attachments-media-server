import { Queue } from "bullmq";
import type { ConnectionOptions } from "bullmq";

// Redis connection config shared between Queue and Worker
export const redisConnection: ConnectionOptions = {
  url: process.env.REDIS_URL,
};

// Queue for watermark processing jobs
export const watermarkQueue = new Queue("watermark", {
  connection: redisConnection,
  defaultJobOptions: {
    removeOnComplete: true,
    removeOnFail: 100, // Keep last 100 failed jobs for debugging
    attempts: 3,
    backoff: {
      type: "exponential",
      delay: 1000,
    },
  },
});
