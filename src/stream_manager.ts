import { DurableObject } from "cloudflare:workers"
import { pickCompactionWindow } from "./compaction"
import { readConfig, type StreamConfig } from "./config"
import {
	decodeLine,
	encodeLine,
	formatOffset,
	offsetEpoch,
	readLines,
	segmentKey,
	streamPrefix,
	type SegmentMetadata,
	type StoredRecord,
} from "./segment"
import { SegmentIndex } from "./segment_index"

/** How long segments replaced by compaction stay in R2 so in-flight reads can finish. */
const TOMBSTONE_GRACE_MS = 24 * 60 * 60 * 1000
/** How long an upload may take before its object counts as orphaned. */
const UPLOAD_GRACE_MS = 60 * 60 * 1000
/** Delay from a flush to the maintenance alarm, so one alarm compacts many flushes. */
const MAINTENANCE_DELAY_MS = 10_000
/** Compaction time per alarm before the alarm reschedules itself. */
const MAINTENANCE_BUDGET_MS = 60_000
/** Most segments one read visits; readers page through the rest. */
const MAX_SEGMENTS_PER_READ = 64
/** Segments fetched from R2 ahead of the one being read. */
const READ_AHEAD = 4
/** R2 deletes at most this many keys per call. */
const R2_DELETE_BATCH = 1000

export type PublishResult =
	{ type: "published"; offsets: string[] } | { type: "version"; version: number } | { type: "fenced"; currentVersion: number }

export type ReadOptions = {
	/** Read records after this offset; "" reads from the beginning. Omit to wait for the next records published. */
	after?: string
	limit: number
	/** How long to wait for new records when none are available. */
	timeoutMs: number
}

type PendingPublish = {
	records: string[]
	resolve: (offsets: string[]) => void
	reject: (error: unknown) => void
}

type Waiter = {
	after: string
	limit: number
	resolve: (records: StoredRecord[]) => void
}

/**
 * One stream. Publishes are buffered for `flushIntervalMs`, then written to R2 as one segment and
 * committed to the SQLite index. Long-polling readers receive each committed batch from memory.
 * An alarm compacts small segments and deletes garbage in the background.
 */
export class StreamManager extends DurableObject<Env> {
	private readonly config: StreamConfig
	private readonly prefix: string
	private index: SegmentIndex
	/** The newest committed offset, or "" when the stream is empty. */
	private lastOffset: string
	/** Epoch of the newest flush; each flush takes a strictly greater one. */
	private epoch: number
	private pending: PendingPublish[] = []
	private flushTimer: ReturnType<typeof setTimeout> | undefined
	/** Runs flushes and deletion one at a time, so segments commit in offset order. */
	private queue: Promise<void> = Promise.resolve()
	private readonly waiters = new Set<Waiter>()

	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env)
		const name = ctx.id.name
		if (name === undefined) {
			throw new Error("StreamManager must be addressed by name")
		}
		this.config = readConfig(env)
		this.prefix = streamPrefix(name)
		this.index = new SegmentIndex(ctx.storage)
		this.lastOffset = this.index.lastOffset()
		this.epoch = this.lastOffset ? offsetEpoch(this.lastOffset) : 0
	}

	/**
	 * Appends `records`, each the JSON text of one record, and resolves with their offsets once they are
	 * durable. `version` is an optional fencing token: publishes with a version below the highest seen
	 * are rejected, and a higher version replaces it. With no records, only the version is updated.
	 */
	async publish(records: string[], version?: number): Promise<PublishResult> {
		if (version !== undefined) {
			const current = this.index.producerVersion()
			if (version < current) {
				return { type: "fenced", currentVersion: current }
			}
			if (version > current) {
				this.index.setProducerVersion(version)
			}
		}
		if (records.length === 0) {
			return { type: "version", version: this.index.producerVersion() }
		}

		const offsets = await new Promise<string[]>((resolve, reject) => {
			this.pending.push({ records, resolve, reject })
			this.scheduleFlush()
		})
		return { type: "published", offsets }
	}

	/** Reads up to `limit` records, waiting up to `timeoutMs` for new ones when none are available. */
	async read({ after, limit, timeoutMs }: ReadOptions): Promise<StoredRecord[]> {
		after ??= this.lastOffset
		const records = await this.readCommitted(after, limit)
		if (records.length > 0 || timeoutMs <= 0) {
			return records
		}
		if (this.lastOffset > after) {
			// A flush committed while the read was in flight.
			return this.readCommitted(after, limit)
		}
		return this.waitForRecords(after, limit, timeoutMs)
	}

	/** Deletes every record and all stream state. Publishes still pending afterwards start the new stream. */
	async destroy(): Promise<void> {
		await this.serialize(async () => {
			await this.ctx.storage.deleteAlarm()
			let cursor: string | undefined
			do {
				const page = await this.env.SEGMENTS.list({ prefix: this.prefix, cursor, limit: R2_DELETE_BATCH })
				if (page.objects.length > 0) {
					await this.env.SEGMENTS.delete(page.objects.map((object) => object.key))
				}
				cursor = page.truncated ? page.cursor : undefined
			} while (cursor !== undefined)
			await this.ctx.storage.deleteAll()

			this.index = new SegmentIndex(this.ctx.storage)
			this.lastOffset = ""
			for (const waiter of this.waiters) {
				waiter.resolve([])
			}
		})
	}

	/** Maintenance: deletes due garbage, then compacts until no window is ready or the budget runs out. */
	async alarm(): Promise<void> {
		await this.collectGarbage()

		const deadline = Date.now() + MAINTENANCE_BUDGET_MS
		let window = pickCompactionWindow(this.index.segments(), this.config.compaction)
		while (window.length > 0 && Date.now() < deadline) {
			await this.compact(window)
			window = pickCompactionWindow(this.index.segments(), this.config.compaction)
		}

		const next = window.length > 0 ? Date.now() : this.index.nextGarbageAt()
		if (next !== null) {
			await this.scheduleAlarm(next)
		}
	}

	private scheduleFlush(): void {
		if (this.flushTimer !== undefined) {
			return
		}
		this.flushTimer = setTimeout(() => {
			this.flushTimer = undefined
			this.serialize(() => this.flush()).catch((error) => console.error("flush failed", error))
		}, this.config.flushIntervalMs)
	}

	private serialize(task: () => Promise<void>): Promise<void> {
		const run = this.queue.then(task)
		this.queue = run.catch(() => {})
		return run
	}

	/** Writes every pending publish as one segment, then resolves the publishers and wakes waiting readers. */
	private async flush(): Promise<void> {
		const batch = this.pending
		this.pending = []
		if (batch.length === 0) {
			return
		}

		this.epoch = Math.max(Date.now(), this.epoch + 1)
		const records: StoredRecord[] = []
		const offsets = batch.map((publish) =>
			publish.records.map((data) => {
				const offset = formatOffset(this.epoch, records.length)
				records.push({ offset, data })
				return offset
			}),
		)
		const body = new TextEncoder().encode(records.map(encodeLine).join(""))
		const segment: SegmentMetadata = {
			firstOffset: records[0].offset,
			lastOffset: records[records.length - 1].offset,
			records: records.length,
			bytes: body.byteLength,
			level: 0,
		}
		const key = segmentKey(this.prefix, segment)

		try {
			this.index.trackUpload(key, Date.now() + UPLOAD_GRACE_MS)
			await this.env.SEGMENTS.put(key, body)
			this.index.commit(segment, key)
		} catch (error) {
			for (const publish of batch) {
				publish.reject(error)
			}
			// Maintenance deletes the upload if it reached R2.
			await this.scheduleMaintenance()
			throw error
		}

		this.lastOffset = segment.lastOffset
		batch.forEach((publish, i) => publish.resolve(offsets[i]))
		for (const waiter of this.waiters) {
			const available = records.filter((record) => record.offset > waiter.after)
			if (available.length > 0) {
				waiter.resolve(available.slice(0, waiter.limit))
			}
		}
		await this.scheduleMaintenance()
	}

	private scheduleMaintenance(): Promise<void> {
		return this.scheduleAlarm(Date.now() + MAINTENANCE_DELAY_MS)
	}

	/** Sets the alarm for `at` unless it is already set to go off sooner. */
	private async scheduleAlarm(at: number): Promise<void> {
		const current = await this.ctx.storage.getAlarm()
		if (current === null || current > at) {
			await this.ctx.storage.setAlarm(at)
		}
	}

	/** Reads up to `limit` committed records after `after`, following segments in offset order. */
	private async readCommitted(after: string, limit: number): Promise<StoredRecord[]> {
		const segments: SegmentMetadata[] = []
		let available = 0
		for (const segment of this.index.segmentsAfter(after, MAX_SEGMENTS_PER_READ)) {
			if (available >= limit) {
				break
			}
			segments.push(segment)
			// Only the first segment can straddle `after`; it holds at least one record after it.
			available += segment.firstOffset > after ? segment.records : 1
		}

		const objects: Promise<R2ObjectBody | null>[] = []
		const records: StoredRecord[] = []
		let read = 0
		try {
			while (read < segments.length && records.length < limit) {
				while (objects.length < Math.min(segments.length, read + READ_AHEAD)) {
					objects.push(this.env.SEGMENTS.get(segmentKey(this.prefix, segments[objects.length])))
				}
				const object = await objects[read]
				if (object === null) {
					throw new Error(`segment ${segmentKey(this.prefix, segments[read])} is missing from R2`)
				}
				read++
				for await (const line of readLines(object.body)) {
					const record = decodeLine(line)
					if (record.offset > after) {
						records.push(record)
						if (records.length >= limit) {
							break
						}
					}
				}
			}
		} finally {
			for (const unread of objects.slice(read)) {
				unread.then(
					(object) => object?.body.cancel(),
					() => {},
				)
			}
		}
		return records
	}

	private waitForRecords(after: string, limit: number, timeoutMs: number): Promise<StoredRecord[]> {
		return new Promise((resolve) => {
			const waiter: Waiter = {
				after,
				limit,
				resolve: (records) => {
					clearTimeout(timer)
					this.waiters.delete(waiter)
					resolve(records)
				},
			}
			const timer = setTimeout(() => waiter.resolve([]), timeoutMs)
			this.waiters.add(waiter)
		})
	}

	/** Merges adjacent `inputs` into one segment a level up. */
	private async compact(inputs: SegmentMetadata[]): Promise<void> {
		const merged: SegmentMetadata = {
			firstOffset: inputs[0].firstOffset,
			lastOffset: inputs[inputs.length - 1].lastOffset,
			records: inputs.reduce((sum, input) => sum + input.records, 0),
			bytes: inputs.reduce((sum, input) => sum + input.bytes, 0),
			level: inputs[0].level + 1,
		}
		const key = segmentKey(this.prefix, merged)
		const inputKeys = inputs.map((input) => segmentKey(this.prefix, input))

		this.index.trackUpload(key, Date.now() + UPLOAD_GRACE_MS)
		await this.concatenate(inputKeys, key, merged.bytes)
		if (!this.index.replace(inputs, inputKeys, merged, key, Date.now() + TOMBSTONE_GRACE_MS)) {
			// The stream was deleted during the merge, along with the garbage entry for `key`.
			await this.env.SEGMENTS.delete(key)
		}
	}

	/** Streams the objects at `inputKeys`, in order, into one new object at `key`. */
	private async concatenate(inputKeys: string[], key: string, bytes: number): Promise<void> {
		const { readable, writable } = new FixedLengthStream(bytes)
		const upload = this.env.SEGMENTS.put(key, readable)
		const copy = async () => {
			try {
				for (const inputKey of inputKeys) {
					const object = await this.env.SEGMENTS.get(inputKey)
					if (object === null) {
						throw new Error(`segment ${inputKey} is missing from R2`)
					}
					await object.body.pipeTo(writable, { preventClose: true })
				}
				await writable.close()
			} catch (error) {
				await writable.abort(error).catch(() => {})
				throw error
			}
		}
		await Promise.all([upload, copy()])
	}

	private async collectGarbage(): Promise<void> {
		let keys: string[]
		while ((keys = this.index.dueGarbage(Date.now(), R2_DELETE_BATCH)).length > 0) {
			await this.env.SEGMENTS.delete(keys)
			this.index.removeGarbage(keys)
		}
	}
}
