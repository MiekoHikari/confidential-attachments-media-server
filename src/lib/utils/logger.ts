export function log(context: string, message: string) {
	console.log(`[${context.toUpperCase()}] ${new Date().toISOString()} - ${message}`);
}
