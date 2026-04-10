#!/usr/bin/env node
/**
 * Injects the CSS import into the built index bundle so that when hosts
 * import @querypanel/react-sdk they get the CSS without importing it manually.
 * tsup extracts CSS to dist/index.css but does not add the import to the JS.
 */
import { readFileSync, writeFileSync } from "fs";
import { fileURLToPath } from "url";
import { dirname, join } from "path";

const __dirname = dirname(fileURLToPath(import.meta.url));
const dist = join(__dirname, "..", "dist");

for (const file of ["index.mjs", "index.js"]) {
  const path = join(dist, file);
  let content = readFileSync(path, "utf8");
  const isEsm = file.endsWith(".mjs");
  const cssImport = isEsm ? "import './index.css';\n" : "require('./index.css');\n";
  if (content.includes("index.css")) continue;
  content = cssImport + content;
  writeFileSync(path, content);
}
