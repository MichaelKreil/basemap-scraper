"use strict"

import polygonClipping from "polygon-clipping";
import * as turf from "@turf/turf";

export function getBBoxTiles(bbox, z) {
	bbox = bboxGeo2Tiles(bbox, z);
	const tiles = [];
	for (let x = bbox[0]; x <= bbox[2]; x++) {
		for (let y = bbox[1]; y <= bbox[3]; y++) {
			tiles.push({ x, y, z });
		}
	}
	return tiles;
}
export function bboxGeo2Tiles(bbox, z) {
	const tileMin = deg2tile(bbox[0], bbox[3], z).map(Math.floor);
	const tileMax = deg2tile(bbox[2], bbox[1], z).map(Math.floor);
	return [tileMin[0], tileMin[1], tileMax[0], tileMax[1]];
}

export function deg2tile(lon_deg, lat_deg, zoom) {
	let n = 2 ** zoom
	return [
		(lon_deg + 180) / 360 * n,
		(1 - Math.asinh(Math.tan(lat_deg * Math.PI / 180)) / Math.PI) / 2 * n
	]
}

export function demercator(feature, size) {
	feature = Object.assign({}, feature);
	feature.geometry = Object.assign({}, feature.geometry);
	feature.properties = Object.assign({}, feature.properties);
	let geo = feature.geometry;
	switch (geo.type) {
		case 'Point': geo.coordinates = demercatorRec(geo.coordinates, 1); break;
		case 'LineString': geo.coordinates = demercatorRec(geo.coordinates, 2); break;
		case 'MultiLineString': geo.coordinates = demercatorRec(geo.coordinates, 3); break;
		case 'Polygon': geo.coordinates = demercatorRec(geo.coordinates, 3); break;
		case 'MultiPolygon': geo.coordinates = demercatorRec(geo.coordinates, 4); break;
		default: throw Error(geo.type);
	}
	feature = turf.rewind(feature, { mutate: true });
	return feature;

	function demercatorRec(coordinates, depth) {
		if (depth > 1) return coordinates.map(l => demercatorRec(l, depth - 1));
		return [
			360 * coordinates[0] / size - 180,
			360 / Math.PI * Math.atan(Math.exp((1 - coordinates[1] * 2 / size) * Math.PI)) - 90,
		]
	}
}

export function intersect(f1, f2) {
	return coords2Feature(polygonClipping.intersection(features2Coords([f1]), features2Coords([f2])));
}

function features2Coords(features) {
	let coords = [];
	for (let feature of features) {
		if (!feature) continue;
		try {
			feature = turf.rewind(feature, { mutate: true })
		} catch (e) {
			console.dir({ feature }, { depth: 10 });
			throw e;
		}
		switch (feature.geometry.type) {
			case 'Polygon': coords.push(feature.geometry.coordinates); continue
			case 'MultiPolygon': coords = coords.concat(feature.geometry.coordinates); continue
		}
		throw Error(feature.geometry.type);
	}
	return coords;
}

function coords2Feature(coords) {
	let outside = [];
	let inside = [];

	coords.forEach(polygon =>
		polygon.forEach(ring =>
			(turf.booleanClockwise(ring) ? inside : outside).push(ring)
		)
	)

	if (outside.length === 1) {
		return turf.polygon(outside.concat(inside));
	} else if (inside.length === 0) {
		return turf.multiPolygon(outside.map(p => [p]));
	} else {
		coords.forEach(polygon => polygon.forEach((ring, index) => {
			if (turf.booleanClockwise(ring) === (index === 0)) ring.reverse();
		}))
		return turf.multiPolygon(coords);
	}
}
