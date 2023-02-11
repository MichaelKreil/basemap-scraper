"use strict"

import { mkdirSync, writeFileSync, existsSync, readFileSync } from "fs";
import { resolve, dirname } from "path";
import http from "http";
import https from "https";
import { Progress } from "./progress.js";
import { getBBoxTiles } from "./geometry.js";

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
	const { level, bbox, tiles_url, tiles_folder } = config;

	mkdirSync(tiles_folder, { recursive: true });

	const todos = getBBoxTiles(bbox, level);
	todos.forEach(t => t.r = Math.random());
	todos.sort((a,b) => a.r - b.r);

	const showProgress = Progress(todos.length);

	await todos.forEachParallel(8, async ({ x, y, z }, i) => {
		showProgress(i);

		const url = tiles_url.replace(/{x}/, x).replace(/{y}/, y).replace(/{z}/, z);
		const filename = resolve(tiles_folder, `${z}/${x}/${y}.pbf`);

		await fetchCached(filename, url, {'Referer': 'https://adv-smart.de/map-editor/map'});
	})
}

async function fetchCached(filename, url, headers) {
	if (existsSync(filename)) return readFileSync(filename);

	ensureFolder(dirname(filename));

	let buffer;
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

			console.log('error', { code, url, filename });

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
	writeFileSync(filename, buffer);
	return buffer;
}

function fetch(url, headers) {
	return new Promise(async (resolve, reject) => {
		let protocol = url.startsWith('https') ? https : http;
		let canceled = false;
		let timeout = setTimeout(() => {
			canceled = true;
			request.destroy();
			return reject(404);
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
