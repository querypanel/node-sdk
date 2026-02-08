import { defineConfig } from "tsup";

export default defineConfig({
	entry: ["src/index.ts"],
	format: ["esm", "cjs"],
	dts: { compilerOptions: { moduleResolution: "bundler" } },
	sourcemap: true,
	clean: true,
	target: "es2022",
	splitting: false,
	minify: false,
	platform: "node",
});
