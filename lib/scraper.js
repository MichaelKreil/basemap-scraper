"use strict"

import { mkdirSync, writeFileSync, existsSync, readFileSync } from "fs";
import { resolve, dirname } from "path";
import http from "http";
import https from "https";
import { Progress } from "./progress.js";
import { getBBoxTiles } from "./geometry.js";
import "big-data-tools";

[http, https].forEach(h => {
	h.myAgent = new h.Agent({
		keepAlive: true,
		keepAliveMsecs: 60000,
		timeout: 3000,
		maxSockets: 8,
		maxFreeSockets: 8,
	})
});

export async function scrape_tiles(config) {
	console.log('scrape tiles');
	
	const { level, bbox, tiles_url, folder1_tiles } = config;

	const lock_file = resolve(folder1_tiles, 'finished.lock');
	if (existsSync(lock_file)) return;

	const todos = getBBoxTiles(bbox, level);
	todos.forEach(t => t.r = Math.random());
	todos.sort((a, b) => a.r - b.r);

	const showProgress = Progress(todos.length);

	await todos.forEachParallel(8, async ({ x, y, z }, i) => {
		showProgress(i);

		const url = tiles_url.replace(/{x}/, x).replace(/{y}/, y).replace(/{z}/, z);
		const filename = resolve(folder1_tiles, `${z}/${x}/${y}.pbf`);

		await fetchAndCache(filename, url, { 'Referer': 'https://adv-smart.de/map-editor/map' });
	})

	writeFileSync(lock_file, '');
}

async function fetchAndCache(filename, url, headers) {
	if (existsSync(filename)) return;

	ensureFolder(dirname(filename));

	let buffer = false;
	for (let i = 1; i <= 3; i--) {
		//let timeStart = Date.now();
		try {
			buffer = await fetch(url, headers);
			process.stderr.write('\u001b[38;5;46m.\u001b[0m');
			//await wait(200);
			break;
		} catch (code) {
			//console.log(code, Date.now() - timeStart, url);
			if (code === 404) {
				buffer = Buffer.allocUnsafe(0);
				break;
			}

			await wait(3000);

			if (code === 408) continue; // timeout

			console.log('error', { code, url, filename });
			throw Error('panic!!!');

			if (code === 500) {
				process.stderr.write('\u001b[38;5;214m-\u001b[0m');
				buffer = Buffer.allocUnsafe(0);
				break;
			}

			if (code === -1) {
				process.stderr.write('\u001b[38;5;208mT\u001b[0m');
				//console.log('ETIMEDOUT, retrying', url)
				continue;
			}

			if (code !== 500) {
				process.stderr.write('\u001b[38;5;196mE\u001b[0m');
				console.log({ url });
				throw Error('Status code: ' + code)
			}
		}
		throw Error('3 failed attempts')
	}
	if (buffer === false) throw Error('panic!!!');

	writeFileSync(filename, buffer);
}

function fetch(url, headers) {
	return new Promise(async (resolve, reject) => {
		let protocol = url.startsWith('https') ? https : http;
		let canceled = false;
		let timeout = setTimeout(() => {
			canceled = true;
			request.destroy();
			return reject(408);
		}, 3000);
		let request = protocol.get(url, { agent: protocol.myAgent, headers, timeout: 3000 }, response => {
			if (canceled) return;
			clearTimeout(timeout);
			if (response.statusCode !== 200) {
				if (response.statusCode !== 404) {
					console.log({
						statusCode: response.statusCode,
						headers: response.headers,
					});
				}
				request.destroy();
				return reject(response.statusCode);
			}
			let buffers = [];
			response.on('data', chunk => buffers.push(chunk));
			response.once('end', () => resolve(Buffer.concat(buffers)))
		}).on('error', async error => {
			if (error.code === 'ETIMEDOUT') return reject(-1);
			if (error.code === 'ENOTFOUND') return reject(-1);
			if (error.code === 'ECONNRESET' && canceled) return;
			throw Error(error);
		})
	})
}

function ensureFolder(folder) {
	if (!existsSync(folder)) {
		ensureFolder(dirname(folder));
		mkdirSync(folder, { recursive: true });
	}
}

function wait(milliseconds) {
	return new Promise(res => setTimeout(res, milliseconds));
}
