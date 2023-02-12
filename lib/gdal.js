"use strict"

import gdal from "gdal-async";
import { existsSync, readdirSync, rmSync, writeFileSync } from "fs";
import { basename, resolve } from "path";
import { spawn } from "child_process";
import "big-data-tools";

export async function cleanup_layers(config) {
	console.log('convert GeoJSON to GPKG');
	await json2gpkg(config.folder2_geojson, config.folder3_gpkg);

	console.log('union features');
	await unionFeatures(config.folder3_gpkg, config.folder4_geojson);

	console.log('convert GeoJSON to GPKG');
	await json2gpkg(config.folder4_geojson, config.folder5_gpkg);
}

async function json2gpkg(folder_src, folder_dst) {
	const lock_file = resolve(folder_dst, 'finished.lock');
	if (existsSync(lock_file)) return;

	await readdirSync(folder_src).forEachParallel(async filename => {
		if (!filename.endsWith('.geojsonl.gz')) return;
		//if (!filename.startsWith('rel')) return;

		const name = basename(filename, '.geojsonl.gz');

		let type;
		if (name.endsWith('punkt') || (name == 'adresse')) {
			type = 'POINT'
		} else if (name.endsWith('linie')) {
			type = 'LINESTRING'
		} else if (name.endsWith('flaeche')) {
			type = 'POLYGON'
		}
		if (!type) throw Error(name);

		const fullname_src = `${folder_src}/${name}.geojsonl.gz`;
		const fullname_dst = `${folder_dst}/${name}.gpkg`;

		if (existsSync(fullname_dst)) rmSync(fullname_dst);

		console.log(`   ${name} started`)
		const cp = spawn('ogr2ogr', [
			'-nln', name,
			'-lco', 'GEOMETRY_NAME=geometry',
			'-explodecollections',
			'-makevalid',
			'-nlt', type,
			fullname_dst,
			'/vsigzip/'+fullname_src
		])
		cp.stdout.on('data', (data) => console.log(`   ${name}/stdout: ${data}`));
		cp.stderr.on('data', (data) => console.error(`   ${name}/stderr: ${data}`));

		await new Promise(res => cp.on('close', code => {
			console.log(`   ${name} finished with code ${code}`);
			res();
		}))
	})

	writeFileSync(lock_file, '');
}
