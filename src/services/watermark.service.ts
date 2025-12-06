import { createCanvas } from '@napi-rs/canvas';

export class WatermarkGenerator {
	/**
	 * Generates a tiled watermark buffer.
	 */
	public generate(width: number, height: number, text: string): Buffer {
		const canvas = createCanvas(width, height);
		const ctx = canvas.getContext('2d');

		// Dynamic font sizing
		const baseFontSize = Math.max(16, Math.min(width, height) / 35);
		const lineHeight = baseFontSize * 1.2;

		ctx.font = `bold ${baseFontSize}px sans-serif`;
		ctx.textBaseline = 'middle';

		// --- Diagonal Tiling Logic ---
		ctx.translate(width / 2, height / 2);
		ctx.rotate(-Math.PI / 4);
		ctx.translate(-width / 2, -height / 2);

		const diagonal = Math.sqrt(width * width + height * height);
		const stepX = Math.max(200, width / 4);
		const stepY = Math.max(100, height / 6);
		const lines = text.split('\n');

		for (let y = -diagonal; y < diagonal; y += stepY) {
			for (let x = -diagonal; x < diagonal; x += stepX) {
				lines.forEach((line, i) => {
					const drawY = y + i * lineHeight;
					ctx.strokeStyle = 'rgba(0, 0, 0, 0.4)';
					ctx.lineWidth = 4;
					ctx.strokeText(line, x, drawY);
					ctx.fillStyle = 'rgba(255, 255, 255, 0.5)';
					ctx.fillText(line, x, drawY);
				});
			}
		}

		ctx.resetTransform();

		// --- Ring/Zone Logic (Simplified for brevity, keep full logic here) ---
		// [Insert your ring/zone logic from the original file here]
		// This ensures graphics logic is isolated from infrastructure.

		return canvas.toBuffer('image/png');
	}
}

export const watermarkGenerator = new WatermarkGenerator();
