// Generated by Wrangler by running `wrangler types`

interface Env {
	StreamManager: DurableObjectNamespace<import("./src/stream_manager").StreamManager>
	StreamData: R2Bucket
	AUTH_HEADER?: string
}
