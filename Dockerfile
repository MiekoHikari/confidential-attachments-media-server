# Build stage
FROM node:20-alpine AS builder

# Install build dependencies for @napi-rs/canvas
RUN apk add --no-cache \
    python3 \
    make \
    g++ \
    libc6-compat

# Install pnpm
RUN corepack enable && corepack prepare pnpm@latest --activate

WORKDIR /app

# Copy package files
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./

# Install dependencies
RUN pnpm install --frozen-lockfile

# Copy source files
COPY tsconfig.json ./
COPY src ./src

# Build the application
RUN pnpm build

# Production stage
FROM node:20-alpine AS production

# Install runtime dependencies for @napi-rs/canvas and ffmpeg
RUN apk add --no-cache \
    ffmpeg \
    libc6-compat \
    fontconfig \
    freetype \
    ttf-dejavu \
    ttf-liberation \
    font-noto

# Install pnpm
RUN corepack enable && corepack prepare pnpm@latest --activate

WORKDIR /app

# Copy package files
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./

# Install production dependencies only
RUN pnpm install --frozen-lockfile --prod

# Copy built files from builder stage
COPY --from=builder /app/dist ./dist

# Set environment to production
ENV NODE_ENV=production

# Expose port (adjust if your app uses a different port)
EXPOSE 3000

# Start the application
CMD ["node", "dist/index.js"]
