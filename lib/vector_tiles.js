"use strict"

import { Progress } from "./progress.js";
import { getBBoxTiles, demercator, intersect } from "./geometry.js";
import { VectorTile } from "@mapbox/vector-tile";
import Protobuf from "pbf";
import { resolve } from "path";
import { createGzip } from "zlib";
import * as turf from "@turf/turf";
import { existsSync, readFileSync, createWriteStream } from "fs";

const TILE_PIXEL_SIZE = 4096;

export async function merge_tiles_to_layers(config) {
	const { level, bbox, folder1_tiles, folder2_geojson } = config;

	const lock_file = resolve(folder2_geojson, 'finished.lock');
	if (existsSync(lock_file)) return;

	const tiles = getBBoxTiles(bbox, level);
	tiles.forEach(t => t.r = Math.random());
	tiles.sort((a, b) => a.r - b.r);

	const showProgress = Progress(tiles.length);
	const layerFiles = new LayerFiles();

	for (let i = 0; i < tiles.length; i++) {
		let tile = tiles[i];
		showProgress(i);

		const filename = resolve(folder1_tiles, `${tile.z}/${tile.x}/${tile.y}.pbf`);
		if (!existsSync(filename)) continue;

		await convertTile(tile.x, tile.y, tile.z, readFileSync(filename));
	}

	await layerFiles.closeAll();

	writeFileSync(lock_file, '');

	async function convertTile(x0, y0, z0, buffer) {

		const bboxPixel = [
			x0 * TILE_PIXEL_SIZE,
			y0 * TILE_PIXEL_SIZE,
			(x0 + 1) * TILE_PIXEL_SIZE,
			(y0 + 1) * TILE_PIXEL_SIZE,
		]

		const bboxPixelPolygon = turf.bboxPolygon(bboxPixel);
		const tile = new VectorTile(new Protobuf(buffer));

		for (let [layerName, layer] of Object.entries(tile.layers)) {
			if (layerName === 'hintergrund') continue;
			
			for (let i = 0; i < layer.length; i++) {
				let feature = featureToObject(layer.feature(i));

				if (!feature) continue;
				if (feature.geometry.coordinates.length === 0) continue;

				let properties = feature.properties;
				properties.layerName = layerName;

				switch (feature.geometry.type) {
					case 'Point':
						let p = feature.geometry.coordinates;
						if (p[0] < bboxPixel[0]) continue;
						if (p[1] < bboxPixel[1]) continue;
						if (p[0] > bboxPixel[2]) continue;
						if (p[1] > bboxPixel[3]) continue;
						feature.properties = Object.assign({}, properties)
						await writeResult(feature);
						continue;
					case 'LineString':
					case 'MultiLineString':
						feature = turf.bboxClip(feature, bboxPixel)
						break;
					case 'Polygon':
					case 'MultiPolygon':
						feature = intersect(feature, bboxPixelPolygon);
						break;
					default: throw Error(feature.geometry.type);
				}

				feature = turf.truncate(feature, { precision: 0, coordinates: 2, mutate: true });

				for (feature of turf.flatten(feature).features) {
					feature.properties = Object.assign({}, properties)
					await writeResult(feature);
				}
			}
		}

		async function writeResult(feature) {
			feature = demercator(feature, (2 ** level) * TILE_PIXEL_SIZE);
			delete feature.bbox;
			await layerFiles.get(feature.properties.layerName).write(JSON.stringify(feature) + '\n');
		}

		function featureToObject(feature) {
			if (feature.extent !== 4096) throw Error();

			let i, j, coordinates = feature.loadGeometry();

			function handleLine(line) {
				for (let i = 0; i < line.length; i++) {
					let p = line[i];
					line[i] = [p.x + bboxPixel[0], p.y + bboxPixel[1]];
				}
			}

			let type;
			switch (feature.type) {
				case 1:
					for (i = 0; i < coordinates.length; i++) coordinates[i] = coordinates[i][0];
					handleLine(coordinates);
					type = 'Point';
					break;

				case 2:
					for (i = 0; i < coordinates.length; i++) handleLine(coordinates[i]);
					type = 'LineString';
					break;

				case 3:
					coordinates = classifyRings(coordinates);
					for (i = 0; i < coordinates.length; i++) {
						for (j = 0; j < coordinates[i].length; j++) handleLine(coordinates[i][j]);
					}
					type = 'Polygon';
					break;
				default: throw Error();
			}

			if (coordinates.length === 1) {
				coordinates = coordinates[0];
			} else {
				type = 'Multi' + type;
			}

			return {
				type: 'Feature',
				geometry: { type, coordinates },
				properties: feature.properties,
			}

			// classifies an array of rings into polygons with outer rings and holes

			function classifyRings(rings) {
				let len = rings.length;

				if (len <= 1) return [rings];

				let polygons = [],
					polygon,
					ccw;

				for (let i = 0; i < len; i++) {
					let area = signedArea(rings[i]);
					if (area === 0) continue;

					if (ccw === undefined) ccw = area < 0;

					if (ccw === area < 0) {
						if (polygon) polygons.push(polygon);
						polygon = [rings[i]];

					} else {
						polygon.push(rings[i]);
					}
				}
				if (polygon) polygons.push(polygon);

				return polygons;
			}

			function signedArea(ring) {
				let sum = 0;
				for (let i = 0, len = ring.length, j = len - 1, p1, p2; i < len; j = i++) {
					p1 = ring[i];
					p2 = ring[j];
					sum += (p2.x - p1.x) * (p1.y + p2.y);
				}
				return sum;
			}
		}
	}

	function LayerFiles() {
		let map = new Map();
		return { get, closeAll }
		function get(name) {
			if (map.has(name)) return map.get(name);

			let filename = resolve(folder2_geojson, name.toLowerCase().replace(/\s/g, '_') + '.geojsonl.gz');
			let fileStream = createWriteStream(filename);
			let gzipStream = createGzip({ level: 5 });
			gzipStream.pipe(fileStream);

			let obj = {
				write: chunk => new Promise(res => {
					if (gzipStream.write(chunk)) return res();
					gzipStream.once('drain', res);
				}),
				close: () => new Promise(res => {
					fileStream.once('close', res);
					gzipStream.end()
				})
			}
			map.set(name, obj);
			return obj;
		}
		async function closeAll() {
			for (let file of map.values()) await file.close();
		}
	}
}
