// Unless explicitly defined, set NODE_ENV as development:
process.env.NODE_ENV ??= "development";

import { setup } from "@skyra/env-utilities";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));

export const rootDir = join(__dirname, "..", "..");
export const srcDir = join(rootDir, "src");

// Read env var
setup({ path: join(srcDir, ".env") });

declare module "@skyra/env-utilities" {
  interface Env {
    AZURE_STORAGE_CONNECTION_STRING: string;

    // REDIS
    REDIS_URL: string;
  }
}
