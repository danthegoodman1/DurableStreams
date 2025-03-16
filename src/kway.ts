/**
 * Merges multiple sorted line readers into a single writer.
 * Each line is assumed to have a record ID in its first 32 characters.
 * The function "peeks" one line from each reader and writes out the smallest ID line,
 * advancing that reader only when its line has been written.
 */
async function kWayMerge(lineReaders: AsyncIterable<string>[], writer: WritableStreamDefaultWriter<Uint8Array>) {
	interface BufferEntry {
		line: string
		iterator: AsyncIterator<string>
	}

	// Initialize each reader by fetching its first line
	const buffers: BufferEntry[] = []
	for (const reader of lineReaders) {
		const iterator = reader[Symbol.asyncIterator]()
		const result = await iterator.next()
		if (!result.done) {
			buffers.push({ line: result.value, iterator })
		}
	}

	// Utility function to extract record ID from a line.
	// In our case the record ID is the first 32 characters.
	const getRecordID = (line: string): string => line.slice(0, 32)

	// Continue until every reader has been exhausted
	while (buffers.length > 0) {
		// Find the buffered line with the smallest record ID.
		let minIndex = 0
		let minRecordID = getRecordID(buffers[0].line)
		for (let i = 1; i < buffers.length; i++) {
			const currID = getRecordID(buffers[i].line)
			if (currID < minRecordID) {
				minRecordID = currID
				minIndex = i
			}
		}
		const smallestEntry = buffers[minIndex]

		// Write the chosen line (append a newline if it wasn't included)
		const buffer = new TextEncoder().encode(smallestEntry.line + "\n")
		await writer.write(buffer)

		// Advance the iterator for the reader we just used.
		const nextResult = await smallestEntry.iterator.next()
		if (nextResult.done) {
			// Remove this reader from our buffers if there are no more lines.
			buffers.splice(minIndex, 1)
		} else {
			buffers[minIndex].line = nextResult.value
		}
	}
	await writer.close()
}
