"use strict"

const UPDATE_EVERY_MSEC = 200;
const HISTORY_IN_MSEC = 60*1000;

export function Progress(n) {
	const MIN_LENGTH = HISTORY_IN_MSEC/UPDATE_EVERY_MSEC;

	let lastTime = 0;
	let times = [];
	return i => {
		let now = Date.now();
		if (now - lastTime < UPDATE_EVERY_MSEC) return;
		lastTime = now;

		if (i > n) i = n;

		times.push([i, now]);

		while (times.length > MIN_LENGTH) times.shift();

		let speed = 0, timeLeft = '?';
		if (times.length > 1) {
			let [i0, t0] = times[0];
			speed = (i - i0) * 1000 / (now - t0);
			timeLeft = (n - i) / speed;
			timeLeft = [
				(Math.floor(timeLeft / 3600)).toString(),
				(Math.floor(timeLeft / 60) % 60 + 100).toString().slice(1),
				(Math.floor(timeLeft) % 60 + 100).toString().slice(1)
			].join(':')
		}
		process.stderr.write(
			'\u001b[2K\r' +
			[
				(100 * i / n).toFixed(2) + '%',
				speed.toFixed(1) + '/s',
				timeLeft
			].map(s => s + ' '.repeat(12 - s.length)).join('')
		);
	}
}
