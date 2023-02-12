"use strict"

import { dirname, resolve } from "path";
import { fileURLToPath } from "url";
import { mkdirSync } from "fs";
import { scrape_tiles } from "./lib/scraper.js";
import { merge_tiles_to_layers } from "./lib/vector_tiles.js";
import { } from "big-data-tools";

start()

async function start() {
	const __dirname = dirname(fileURLToPath(import.meta.url));
	const CONFIG = {
		name: 'basemap.de',
		bbox: [5.8, 47.2, 15.1, 55.1],
		level: 15,
		//tiles_url: 'https://basemap.de/projekt/tiles/smarttiles_de_public_v1/{z}/{x}/{y}.pbf',
		tiles_url: 'https://sgx.geodatenzentrum.de/gdz_basemapde_vektor/tiles/v1/bm_web_de_3857/{z}/{x}/{y}.pbf',
	}

	'1_tiles,2_geojson,3_gpkg,4_geojson,5_gpkg'.split(',').forEach(name => {
		let folder = resolve(__dirname, "cache", CONFIG.name, name);
		mkdirSync(folder, { recursive: true });
		CONFIG['folder'+name] = folder;
	})

	await scrape_tiles(CONFIG);
	await merge_tiles_to_layers(CONFIG);
	await cleanup_layers();
}
