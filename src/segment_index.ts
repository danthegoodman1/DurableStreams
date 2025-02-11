import { RBTree } from "bintrees"
import { DurableObject } from "cloudflare:workers"
import { SegmentMetadata } from "./segment"

const hour = 1000 * 60 * 60
const day = hour * 24

const activeLogSegmentKey = "active_log_segment::" // what logs segments are actually active, used for compaction, tombstone cleaning, and queries
const MaxStaleSegmentMs = day * 1 // 1 day

function buildLogSegmentIndexKey(segmentName: string): string {
	return `${activeLogSegmentKey}${segmentName}`
}

export class SegmentIndex<Env = unknown> extends DurableObject<Env> {
	tree: RBTree<SegmentMetadata>

	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env)

		// We know 2 things to be true about SegmentMetadata:
		// 1. firstOffset is always less than or equal to lastOffset
		// 2. No instances SegmentMetadata can have intersecting offset ranges
		// Therefore we can make decisions entirely off of the first offset
		this.tree = new RBTree<SegmentMetadata>((a, b) => {
			// Return 0 if a == b
			// > 0 if a>b
			// < 0 if a<b
			if (a.firstOffset < b.firstOffset) {
				// a is before b
				return -1
			}
			if (a.firstOffset > b.firstOffset) {
				// a is after b
				return 1
			}

			// should we crash here?
			console.error("2 segments with intersecting offset ranges - this is a consistency bug", a, b)
			return 0
		})
	}

	async buildIndexFromStorage() {
		const segments = await this.ctx.storage.list<SegmentMetadata>({
			prefix: activeLogSegmentKey,
		})

		for (const [_, segment] of segments) {
			this.tree.insert(segment)
		}
	}

	async writeLogSegmentMetadata(metadata: SegmentMetadata) {
		// First we need to durably store it
		await this.ctx.storage.put(buildLogSegmentIndexKey(metadata.name), JSON.stringify(metadata))
		// Then we can add it to the memory index
		this.tree.insert(metadata)
	}
}
