import type { SegmentMetadata } from "./segment"

const SEGMENT_COLUMNS = "first_offset AS firstOffset, last_offset AS lastOffset, records, bytes, level"
const PRODUCER_VERSION_KEY = "producer_version"

/** Thrown inside a transaction to roll it back. */
class Rollback extends Error {}

/**
 * The stream's durable state, kept in the Durable Object's SQLite database.
 *
 * `segments` lists live segments. It is keyed by last offset because reads look up the first segment
 * whose last offset follows the reader's offset.
 *
 * `garbage` lists R2 objects to delete once `delete_at` passes: segments replaced by compaction
 * (kept for a grace period so in-flight reads finish) and uploads that never committed. Every upload
 * is recorded here before it starts and removed when its segment commits, so a crash at any point
 * leaves no untracked objects.
 */
export class SegmentIndex {
	private readonly sql: SqlStorage

	constructor(private readonly storage: DurableObjectStorage) {
		this.sql = storage.sql
		this.sql.exec(`
			CREATE TABLE IF NOT EXISTS segments (
				last_offset TEXT PRIMARY KEY,
				first_offset TEXT NOT NULL,
				records INTEGER NOT NULL,
				bytes INTEGER NOT NULL,
				level INTEGER NOT NULL
			) WITHOUT ROWID;
			CREATE TABLE IF NOT EXISTS garbage (
				key TEXT PRIMARY KEY,
				delete_at INTEGER NOT NULL
			) WITHOUT ROWID;
		`)
	}

	/** The newest committed offset, or "" when the stream is empty. */
	lastOffset(): string {
		const [row] = this.sql.exec<{ last_offset: string }>("SELECT last_offset FROM segments ORDER BY last_offset DESC LIMIT 1").toArray()
		return row?.last_offset ?? ""
	}

	/** Up to `limit` segments holding records after `offset`, oldest first. */
	segmentsAfter(offset: string, limit: number): SegmentMetadata[] {
		return this.sql
			.exec<SegmentMetadata>(`SELECT ${SEGMENT_COLUMNS} FROM segments WHERE last_offset > ? ORDER BY last_offset LIMIT ?`, offset, limit)
			.toArray()
	}

	/** Every segment, oldest first, read lazily. */
	segments(): Iterable<SegmentMetadata> {
		return this.sql.exec<SegmentMetadata>(`SELECT ${SEGMENT_COLUMNS} FROM segments ORDER BY last_offset`)
	}

	/** Records that `key` is being uploaded; it becomes garbage at `deleteAt` unless committed first. */
	trackUpload(key: string, deleteAt: number): void {
		this.sql.exec("INSERT OR REPLACE INTO garbage (key, delete_at) VALUES (?, ?)", key, deleteAt)
	}

	/** Adds a flushed segment uploaded to `key`. */
	commit(segment: SegmentMetadata, key: string): void {
		this.storage.transactionSync(() => {
			this.insert(segment)
			this.sql.exec("DELETE FROM garbage WHERE key = ?", key)
		})
	}

	/**
	 * Replaces `inputs` with `merged`, uploaded to `mergedKey`, and schedules the inputs' objects for
	 * deletion at `deleteInputsAt`. Returns false without changing anything when an input is no longer
	 * live, which happens when the stream was deleted during compaction.
	 */
	replace(inputs: SegmentMetadata[], inputKeys: string[], merged: SegmentMetadata, mergedKey: string, deleteInputsAt: number): boolean {
		try {
			this.storage.transactionSync(() => {
				for (const input of inputs) {
					const deleted = this.sql.exec("DELETE FROM segments WHERE last_offset = ? RETURNING 1", input.lastOffset).toArray()
					if (deleted.length === 0) {
						throw new Rollback()
					}
				}
				this.insert(merged)
				this.sql.exec("DELETE FROM garbage WHERE key = ?", mergedKey)
				for (const key of inputKeys) {
					this.sql.exec("INSERT OR REPLACE INTO garbage (key, delete_at) VALUES (?, ?)", key, deleteInputsAt)
				}
			})
			return true
		} catch (error) {
			if (error instanceof Rollback) {
				return false
			}
			throw error
		}
	}

	/** Up to `limit` garbage keys due for deletion at `now`. */
	dueGarbage(now: number, limit: number): string[] {
		return this.sql
			.exec<{ key: string }>("SELECT key FROM garbage WHERE delete_at <= ? ORDER BY delete_at LIMIT ?", now, limit)
			.toArray()
			.map((row) => row.key)
	}

	removeGarbage(keys: string[]): void {
		this.storage.transactionSync(() => {
			for (const key of keys) {
				this.sql.exec("DELETE FROM garbage WHERE key = ?", key)
			}
		})
	}

	/** When the next garbage becomes due, or null when there is none. */
	nextGarbageAt(): number | null {
		const [row] = this.sql.exec<{ next: number | null }>("SELECT MIN(delete_at) AS next FROM garbage").toArray()
		return row?.next ?? null
	}

	producerVersion(): number {
		return this.storage.kv.get<number>(PRODUCER_VERSION_KEY) ?? 0
	}

	setProducerVersion(version: number): void {
		this.storage.kv.put(PRODUCER_VERSION_KEY, version)
	}

	private insert(segment: SegmentMetadata): void {
		this.sql.exec(
			"INSERT INTO segments (last_offset, first_offset, records, bytes, level) VALUES (?, ?, ?, ?, ?)",
			segment.lastOffset,
			segment.firstOffset,
			segment.records,
			segment.bytes,
			segment.level,
		)
	}
}
