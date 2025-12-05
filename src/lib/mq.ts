import { Queue } from "bullmq";
import type { ConnectionOptions } from "bullmq";

// Redis connection config shared between Queue and Worker
export const redisConnection: ConnectionOptions = {
  url: process.env.REDIS_URL,
  // Improve connection resilience
  maxRetriesPerRequest: null, // Required for BullMQ workers - disables the retry limit
  enableReadyCheck: false, // Prevents blocking when Redis is temporarily unavailable
  retryStrategy: (times: number) => {
    // Exponential backoff with max 30 second delay
    const delay = Math.min(times * 500, 30000);
    console.log(`[REDIS] Reconnecting... attempt ${times}, delay ${delay}ms`);
    return delay;
  },
};

// Queue for watermark processing jobs
export const watermarkQueue = new Queue("watermark", {
  connection: redisConnection,
  defaultJobOptions: {
    attempts: 3,
    backoff: {
      type: "exponential",
      delay: 2000,
    },
    removeOnComplete: 10, // Keep last 10 completed jobs
    removeOnFail: 100, // Keep last 100 failed jobs for debugging
  },
});
