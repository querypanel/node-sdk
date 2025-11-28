import { defineConfig } from "vitest/config";

export default defineConfig({
	test: {
		globals: true,
		environment: "node",
		// Separate timeouts for unit vs integration tests
		testTimeout: process.env.TEST_TYPE === "integration" ? 30000 : 5000,
		hookTimeout: process.env.TEST_TYPE === "integration" ? 30000 : 10000,
		coverage: {
			provider: "v8",
			reporter: ["text", "json", "html"],
			exclude: [
				"node_modules/**",
				"dist/**",
				"**/*.test.ts",
				"**/*.spec.ts",
				"test-clickhouse.ts",
				"test/**",
			],
		},
	},
});
