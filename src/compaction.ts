import type { SegmentMetadata } from "./segment"

export type CompactionLimits = {
	/** Most segments merged at once. At least 2. */
	maxSegments: number
	/** A segment with at least this many records is full and never compacted again. */
	maxRecords: number
	/** A segment with at least this many bytes is full and never compacted again. */
	maxBytes: number
}

export function isFull(segment: SegmentMetadata, limits: CompactionLimits): boolean {
	return segment.records >= limits.maxRecords || segment.bytes >= limits.maxBytes
}

/**
 * Picks the oldest run of adjacent segments to merge, or returns [] when no run is ready.
 *
 * Compaction is tiered, like an LSM tree. Flushes write level-0 segments, and merging a run of
 * `maxSegments` same-level segments yields one segment a level up, so each record is rewritten once
 * per level: about log_maxSegments(maxRecords / records per flush) times. A run is also ready once
 * its combined size reaches a limit, which produces a full segment smaller than twice the limits.
 */
export function pickCompactionWindow(segments: Iterable<SegmentMetadata>, limits: CompactionLimits): SegmentMetadata[] {
	let run: SegmentMetadata[] = []
	let records = 0
	let bytes = 0
	for (const segment of segments) {
		const full = isFull(segment, limits)
		if (full || (run.length > 0 && segment.level !== run[0].level)) {
			run = []
			records = 0
			bytes = 0
		}
		if (full) {
			continue
		}

		run.push(segment)
		records += segment.records
		bytes += segment.bytes
		// A single segment never reaches the size limits here because full segments are skipped.
		if (run.length >= limits.maxSegments || records >= limits.maxRecords || bytes >= limits.maxBytes) {
			return run
		}
	}
	return []
}
