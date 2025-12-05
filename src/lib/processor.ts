import { createCanvas, loadImage } from "canvas";

function createWatermarkBuffer(
  width: number,
  height: number,
  watermark: string
): Buffer {
  const canvas = createCanvas(width, height);
  const ctx = canvas.getContext("2d");

  ctx.font = "bold 24px sans-serif";
  ctx.textBaseline = "middle";

  // Rotate context for diagonal watermarks
  ctx.translate(width / 2, height / 2);
  ctx.rotate(-Math.PI / 4);
  ctx.translate(-width / 2, -height / 2);

  const diagonal = Math.sqrt(width * width + height * height);
  const stepX = 350;
  const stepY = 180;

  const lines = watermark.split("\n");
  const lineHeight = 21;

  // Tiled watermark pattern
  for (let y = -diagonal; y < diagonal; y += stepY) {
    for (let x = -diagonal; x < diagonal; x += stepX) {
      lines.forEach((line, i) => {
        const drawY = y + i * lineHeight;
        ctx.strokeStyle = "rgba(0, 0, 0, 0.3)";
        ctx.lineWidth = 3;
        ctx.strokeText(line, x, drawY);
        ctx.fillStyle = "rgba(255, 255, 255, 0.4)";
        ctx.fillText(line, x, drawY);
      });
    }
  }

  ctx.resetTransform();

  // Add one prominent watermark at random position
  const textWidth = Math.max(
    ...lines.map((line) => ctx.measureText(line).width)
  );
  const textHeight = lines.length * lineHeight;
  const padding = 20;

  const randomX = padding + Math.random() * (width - textWidth - 2 * padding);
  const randomY =
    padding +
    textHeight +
    Math.random() * (height - 2 * textHeight - 2 * padding);

  lines.forEach((line, i) => {
    const drawY = randomY + i * lineHeight;
    ctx.strokeStyle = "rgba(0, 0, 0, 0.5)";
    ctx.lineWidth = 3;
    ctx.strokeText(line, randomX, drawY);
    ctx.fillStyle = "rgba(0, 255, 255, 0.70)";
    ctx.fillText(line, randomX, drawY);
  });

  return canvas.toBuffer();
}

export async function watermarkImage(
  buffer: Buffer<ArrayBuffer>,
  watermark: string
): Promise<Buffer> {
  const image = await loadImage(buffer);
  const canvas = createCanvas(image.width, image.height);
  const ctx = canvas.getContext("2d");

  ctx.drawImage(image, 0, 0);

  const watermarkBuffer = createWatermarkBuffer(
    image.width,
    image.height,
    watermark
  );

  const watermarkOverlay = await loadImage(watermarkBuffer);
  ctx.drawImage(watermarkOverlay, 0, 0);

  return canvas.toBuffer();
}
