import { describe, expect, it } from "vitest"
import { decodeLine, encodeLine, formatOffset, offsetEpoch, OFFSET_LENGTH, readLines, segmentKey, streamPrefix } from "../src/segment"

function streamOf(chunks: Uint8Array[], onCancel?: () => void): ReadableStream<Uint8Array> {
	return new ReadableStream({
		pull(controller) {
			const chunk = chunks.shift()
			if (chunk === undefined) {
				controller.close()
			} else {
				controller.enqueue(chunk)
			}
		},
		cancel: onCancel,
	})
}

async function collect(stream: ReadableStream<Uint8Array>): Promise<string[]> {
	const lines: string[] = []
	for await (const line of readLines(stream)) {
		lines.push(line)
	}
	return lines
}

const encode = (text: string) => new TextEncoder().encode(text)

describe("offsets", () => {
	it("formats fixed-width offsets that round-trip the epoch", () => {
		const offset = formatOffset(1_739_995_966_373, 42)
		expect(offset).toBe("00017399959663730000000000000042")
		expect(offset).toHaveLength(OFFSET_LENGTH)
		expect(offsetEpoch(offset)).toBe(1_739_995_966_373)
	})

	it("orders offsets as strings the same as numerically", () => {
		const ordered = [formatOffset(9, 0), formatOffset(9, 10), formatOffset(10, 0), formatOffset(10, 1), formatOffset(1_000_000, 0)]
		expect([...ordered].sort()).toEqual(ordered)
	})
})

describe("segment lines", () => {
	it("round-trips records, including JSON with escaped newlines", () => {
		const record = { offset: formatOffset(1, 2), data: JSON.stringify({ text: "line one\nline two " }) }
		const line = encodeLine(record)
		expect(line.endsWith("\n")).toBe(true)
		expect(line.indexOf("\n")).toBe(line.length - 1)
		expect(decodeLine(line.slice(0, -1))).toEqual(record)
	})
})

describe("readLines", () => {
	it("splits lines that span chunk boundaries", async () => {
		const lines = await collect(streamOf([encode("alp"), encode("ha\nbe"), encode("ta\n\ngamma\n")]))
		expect(lines).toEqual(["alpha", "beta", "", "gamma"])
	})

	it("decodes multi-byte characters split across chunks", async () => {
		const bytes = encode("héllo 🌊\nwörld\n")
		const chunks = Array.from(bytes, (byte) => new Uint8Array([byte]))
		expect(await collect(streamOf(chunks))).toEqual(["héllo 🌊", "wörld"])
	})

	it("yields a final line without a trailing newline", async () => {
		expect(await collect(streamOf([encode("one\ntwo")]))).toEqual(["one", "two"])
	})

	it("yields nothing for an empty stream", async () => {
		expect(await collect(streamOf([]))).toEqual([])
	})

	it("cancels the source when the reader stops early", async () => {
		let cancelled = false
		const source = streamOf([encode("one\ntwo\n"), encode("three\n"), encode("four\n")], () => {
			cancelled = true
		})
		for await (const line of readLines(source)) {
			expect(line).toBe("one")
			break
		}
		expect(cancelled).toBe(true)
	})
})

describe("R2 keys", () => {
	it("names segments by offset range under the stream prefix", () => {
		const segment = { firstOffset: formatOffset(1, 0), lastOffset: formatOffset(1, 9) }
		expect(segmentKey(streamPrefix("orders"), segment)).toBe(`orders/${segment.firstOffset}-${segment.lastOffset}.seg`)
	})

	it("gives nested and look-alike stream names disjoint prefixes", () => {
		const names = ["a", "a/b", "a%2Fb", "a/", "ab", "a b"]
		const prefixes = names.map(streamPrefix)
		for (const prefix of prefixes) {
			expect(prefix.indexOf("/")).toBe(prefix.length - 1)
			const overlapping = prefixes.filter((other) => other.startsWith(prefix))
			expect(overlapping).toEqual([prefix])
		}
	})
})
