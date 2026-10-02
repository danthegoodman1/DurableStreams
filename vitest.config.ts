import { cloudflareTest } from "@cloudflare/vitest-plugin"
import { defineConfig } from "vitest/config"

export default defineConfig({
	plugins: [
		cloudflareTest({
			wrangler: { configPath: "./wrangler.jsonc" },
			miniflare: {
				bindings: {
					AUTH_HEADER: "test-secret",
					FLUSH_INTERVAL_MS: "20",
				},
			},
		}),
	],
})
