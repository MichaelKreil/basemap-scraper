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
	await unionFeatures(config.folder3_gpkg, config.folder4_gpkg);
}

async function json2gpkg(folder_src, folder_dst) {
	const lock_file = resolve(folder_dst, 'finished.lock');
	if (existsSync(lock_file)) return;

	await readdirSync(folder_src).forEachParallel(async filename => {
		if (!filename.endsWith('.geojsonl.gz')) return;
		if (filename.startsWith('hintergrund')) return; // ignoriere layer "hintergrund"
		//if (!filename.startsWith('rel')) return;

		const name = basename(filename, '.geojsonl.gz');

		let type = getTypeByName(name);

		const fullname_src = `${folder_src}/${name}.geojsonl.gz`;
		const fullname_dst = `${folder_dst}/${name}.gpkg`;

		if (existsSync(fullname_dst)) rmSync(fullname_dst);

		await ogr2ogr(name, [
			'-nln', name,
			'-lco', 'GEOMETRY_NAME=geometry',
			'-explodecollections',
			'-makevalid',
			'-nlt', type,
			fullname_dst,
			'/vsigzip/'+fullname_src
		])
	})

	writeFileSync(lock_file, '');
}

async function unionFeatures(folder_src, folder_dst) {
	const lock_file = resolve(folder_dst, 'finished.lock');
	if (existsSync(lock_file)) return;

	await readdirSync(folder_src).forEachParallel(async filename => {
		if (!filename.endsWith('.gpkg')) return;
		if (filename.startsWith('hintergrund')) return; // ignoriere layer "hintergrund"
		//if (!filename.startsWith('rel')) return;

		const name = basename(filename, '.gpkg');
		let type = getTypeByName(name);

		const fullname_src = `${folder_src}/${name}.gpkg`;
		const fullname_dst = `${folder_dst}/${name}.gpkg`;

		if (existsSync(fullname_dst)) rmSync(fullname_dst);

		let dataset = await gdal.openAsync(fullname_src);
		let layer = await dataset.layers.getAsync(0);
		let fields = layer.fields.getNames().filter(n => n !== 'layerName').join(', ');

		await ogr2ogr(name, [
			'-dialect', 'SQLite',
			'-sql', `SELECT ST_Union(geometry) AS geometry, ${fields} FROM ${name} GROUP BY ${fields};`,
			'-nln', name,
			'-lco', 'GEOMETRY_NAME=geometry',
			'-explodecollections',
			'-makevalid',
			'-nlt', type,
			fullname_dst,
			fullname_src
		])
	})

	writeFileSync(lock_file, '');
}

function getTypeByName(name) {
	if (name.endsWith('punkt') || (name == 'adresse')) {
		return 'POINT'
	} else if (name.endsWith('linie')) {
		return 'LINESTRING'
	} else if (name.endsWith('flaeche')) {
		return 'POLYGON'
	}
	throw Error(name);
}

function ogr2ogr(name, args) {
	const expectedPString = '0,.,.,.,10,.,.,.,20,.,.,.,30,.,.,.,40,.,.,.,50,.,.,.,60,.,.,.,70,.,.,.,80,.,.,.,90,.,.,.,100'.split(',');
	let pos = 0;
	console.log(`   ${name} started`)

	if (!args.includes('-progress')) args.unshift('-progress')

	const cp = spawn('ogr2ogr', args)
	cp.stdout.on('data', data => {
		data = data.toString();
		while ((data.length > 0) && (pos < 40)) {
			let part = expectedPString[pos];
			if (data.startsWith(part)) {
				data = data.slice(part.length);
				pos++;
			}
		}
		console.log(`   ${name}/progress: ${pos*2.5}%`);
	})
	cp.stderr.on('data', data => {
		if (data.includes('which is not normally allowed by the GeoPackage specification')) return;
		if (data.includes('This can occur if the input geometry is invalid.')) return;
		if (data.includes('GEOS error: TopologyException: side location conflict')) return;
		console.error(`   ${name}/stderr: ${data}`)
	});

	return new Promise((res, rej) => cp.on('close', code => {
		
		console.log(`   ${name} finished with code ${code}`);
		if (code === 0) res(); else rej();
	}))
}
