/**
 * An offset is 32 digits: the 16-digit epoch (Unix ms) of the flush that wrote the record, then a
 * 16-digit counter within that flush. The fixed width makes string order match numeric order.
 */
export const OFFSET_LENGTH = 32
const PART_LENGTH = 16

export function formatOffset(epoch: number, counter: number): string {
	return epoch.toString().padStart(PART_LENGTH, "0") + counter.toString().padStart(PART_LENGTH, "0")
}

export function offsetEpoch(offset: string): number {
	return Number(offset.slice(0, PART_LENGTH))
}

/**
 * Index entry for a segment: one immutable R2 object holding a contiguous run of records.
 * Segments never overlap, so ordering by first offset and by last offset agree.
 */
export type SegmentMetadata = {
	firstOffset: string
	lastOffset: string
	records: number
	bytes: number
	/** 0 for a flushed segment; one more than its inputs' level for a compacted segment. */
	level: number
}

export type StoredRecord = {
	offset: string
	/** The record's JSON text. */
	data: string
}

/**
 * Each line of a segment is a record's offset followed by its JSON text. JSON text never contains a
 * raw newline, so lines split unambiguously, and segments concatenate byte-for-byte.
 */
export function encodeLine(record: StoredRecord): string {
	return `${record.offset}${record.data}\n`
}

export function decodeLine(line: string): StoredRecord {
	return { offset: line.slice(0, OFFSET_LENGTH), data: line.slice(OFFSET_LENGTH) }
}

/**
 * Prefix for a stream's R2 keys. Encoding the name removes every `/`, so no stream's prefix matches
 * another stream's keys (e.g. stream `a` never lists or deletes objects of stream `a/b`).
 */
export function streamPrefix(streamName: string): string {
	return `${encodeURIComponent(streamName)}/`
}

/** Segments are named by their offset range, which makes R2 listings sorted and self-describing. */
export function segmentKey(prefix: string, segment: Pick<SegmentMetadata, "firstOffset" | "lastOffset">): string {
	return `${prefix}${segment.firstOffset}-${segment.lastOffset}.seg`
}

/** Yields each newline-terminated line of `stream`. Stopping early cancels the stream. */
export async function* readLines(stream: ReadableStream<Uint8Array>): AsyncGenerator<string> {
	let partial = ""
	for await (const chunk of stream.pipeThrough(new TextDecoderStream())) {
		let start = 0
		let newline: number
		while ((newline = chunk.indexOf("\n", start)) !== -1) {
			yield partial + chunk.slice(start, newline)
			partial = ""
			start = newline + 1
		}
		partial += chunk.slice(start)
	}
	if (partial) {
		yield partial
	}
}
