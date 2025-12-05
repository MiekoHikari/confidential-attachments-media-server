import z from "zod";

export const newJobSchema = z.object({
  container: z.string().min(1),
  jobId: z.string().min(1),
  type: z.enum(["image", "video"]),
  filename: z.string().min(1),
  responseUrl: z.url(),
  watermarkText: z.string().min(1),
  interaction: z.object({
    applicationId: z.string().min(1),
    token: z.string().min(1),
    messageId: z.string().min(1),
  }),
});
