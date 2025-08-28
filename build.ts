// Clean dist
await Bun.$`rm -rf dist`;

// Build JavaScript
await Bun.build({
  entrypoints: ["src/index.ts"],
  outdir: "./dist",
  target: "node",
  format: "esm",
  sourcemap: true,
  minify: true,
});

// Build TypeScript declarations
await Bun.$`bunx tsc --noEmit false `;

export {};
