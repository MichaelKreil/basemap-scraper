"use strict"

import { dirname, resolve } from "path";
import { fileURLToPath } from "url";
import { scrape_tiles } from "./lib/scraper.js";
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

	CONFIG.tiles_folder = resolve(__dirname, "cache", CONFIG.name, 'tiles');

	await scrape_tiles(CONFIG);
	await merge_tiles_to_layers();
	await cleanup_layers();
}
