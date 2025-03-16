// Need to export DO classes from here
export { StreamManager } from "./stream_manager"

export default {
	async fetch(request, env, ctx): Promise<Response> {
		console.log("fetch", request.url)
		let id = env.StreamManager.idFromName(new URL(request.url).pathname)
		let stub = env.StreamManager.get(id)
		return stub.fetch(request)
	},
} satisfies ExportedHandler<Env>
