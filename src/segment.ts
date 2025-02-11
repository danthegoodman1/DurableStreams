export interface SegmentMetadata {
	firstOffset: string
	lastOffset: string
	createdMS: number
	/**
	 * An optimization to stop the common stream name prefix from being stored
	 * multiple times in the index, however this should never be that large.
	 */
	name: string
}

/**
 * Async generator that yields each line from a ReadableStream.
 */
export async function* readLines(stream: ReadableStream<Uint8Array>) {
	const reader = stream.getReader()
	const decoder = new TextDecoder()
	let { value: chunk, done } = await reader.read()
	let buffer = ""

	while (!done) {
		// Decode the current chunk (streaming mode)
		buffer += decoder.decode(chunk, { stream: true })
		// Split on newline (adjust the regex if you need to handle \r\n)
		let lines = buffer.split("\n")
		// Keep the last (possibly partial) line in the buffer
		buffer = lines.pop() || ""
		// Yield complete lines
		for (const line of lines) {
			yield line
		}
		;({ value: chunk, done } = await reader.read())
	}

	// Process any remaining text
	buffer += decoder.decode() // flush remaining bytes
	if (buffer) {
		yield buffer
	}
}

export function generateLogSegmentName(stream: string, epoch: number) {
	return `${stream}/${epoch}:${crypto.randomUUID()}.seg`
}

export function parseLogSegmentName(name: string): { stream: string; epoch: number; uuid: string } {
	const [stream, parts] = name.split("/")
	const [epoch, uuid] = parts.split(".")[0].split(":")
	return { stream, epoch: Number(epoch), uuid }
}
