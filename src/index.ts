import "./lib/setup.js";
import { serve } from "@hono/node-server";
import { zValidator } from "@hono/zod-validator";
import { Hono } from "hono";
import { logger } from "hono/logger";
import { watermarkQueue } from "./lib/mq.js";
import { newJobSchema } from "./lib/types.js";
import "./lib/processor.js"; // Import to start the worker

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

app.post("/new-item", zValidator("json", newJobSchema), async (c) => {
  const requestId = crypto.randomUUID();
  const jobData = c.req.valid("json");

  console.log(
    `[REQUEST:${requestId}] ${new Date().toISOString()} - New job received`
  );
  console.log(`[REQUEST:${requestId}] Job ID: ${jobData.jobId}`);
  console.log(`[REQUEST:${requestId}] Container: ${jobData.container}`);
  console.log(`[REQUEST:${requestId}] Type: ${jobData.type}`);
  console.log(`[REQUEST:${requestId}] Filename: ${jobData.filename}`);

  // Add job to the queue
  await watermarkQueue.add("watermark", jobData, {
    jobId: jobData.jobId,
  });

  console.log(
    `[REQUEST:${requestId}] Job queued successfully, returning 202 Accepted`
  );

  return c.json({ status: "accepted", jobId: jobData.jobId }, 202);
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
