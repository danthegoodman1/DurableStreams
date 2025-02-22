import { RBTree } from "bintrees"

export interface SegmentMetadata {
	firstOffset: string
	lastOffset: string
	createdMS: number
	/**
	 * An optimization to stop the common stream name prefix from being stored
	 * multiple times in the index would save memory, however this should never be that large.
	 */
	name: string
	records: number
	bytes: number
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

// Compaction rules
const MaxSegments = 10
// Worst case is (2 * MaxRecords) -1
const MaxRecords = 5_000
// Worst case is (2 * MaxBytes) -1
const MaxBytes = 10_000_000 // 10MB

/**
 * calculateCompactWindow is called to calculate the window of segments that should be compacted.
 *
 * It starts at the back, and aggregates segments until it hits one of the thresholds.
 *
 * If it encounters a segment that individually exceeds the threshold, it will return the window if
 * there are 2 or more segments, otherwise it will reset the window and start from the next segment.
 */
export function calculateCompactWindow(tree: RBTree<SegmentMetadata>): SegmentMetadata[] {
	// Compact from the oldest segment to the newest
	const iter = tree.iterator()
	let item: SegmentMetadata | null = null
	// did you know you could do this in JS?
	let segmentWindow: SegmentMetadata[] = []
	let totalBytes = 0
	let totalRecords = 0
	while ((item = iter.next()) !== null) {
		// We keep iterating until we hit a max number of segments, records, or bytes
		// or, the next file already is too large to compact (we skip to start next window)

		// Check the window limits
		if (segmentWindow.length >= MaxSegments) {
			// We hit the max number of segments, so we need to compact
			console.debug("hit max number of segments, compacting")
			break
		}
		if (totalBytes >= MaxBytes) {
			// We hit the max number of bytes, so we need to compact
			console.debug("hit max number of bytes, compacting")
			break
		}
		if (totalRecords >= MaxRecords) {
			// We hit the max number of records, so we need to compact
			console.debug("hit max number of records, compacting")
			break
		}

		// Check if we need to start a new window by skipping, or compact what we have
		if (item.bytes > MaxBytes) {
			if (segmentWindow.length < 2) {
				// We need to skip and start a new window
				console.debug(`next file ${item.name} has too many bytes, skipping because we don't have enough segments to compact`)
				segmentWindow = []
				continue
			}

			// Otherwise we need to compact what we have
			console.debug(`next file ${item.name} has too many bytes, compacting`)
			break
		}
		if (item.records > MaxRecords) {
			if (segmentWindow.length < 2) {
				// We need to skip and start a new window
				console.debug(`next file ${item.name} has too many records, skipping because we don't have enough segments to compact`)
				segmentWindow = []
				continue
			}

			// Otherwise we need to compact what we have
			console.debug(`next file ${item.name} has too many records, compacting`)
			break
		}

		segmentWindow.push(item)
		totalBytes += item.bytes
		totalRecords += item.records
	}

	// We either broke to compact, or we hit the end of the tree

	if (segmentWindow.length < 2) {
		// We don't have enough segments to compact, so we can't compact
		console.debug("not enough segments to compact after iteration, exiting")
		return []
	}

	return segmentWindow
}
