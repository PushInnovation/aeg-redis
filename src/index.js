'use strict';

import redis from 'redis';
import {EventEmitter} from 'events';

const SCAN_LIMIT = 1000;

class Redis extends EventEmitter {

	constructor(options) {
		super();
		this._client = redis.createClient(options);
	}

	get(key, callback) {
		this._client.get(key, callback);
	}

	set(key, value, callback) {
		this._client.set(key, value, callback);
	}

	del(key, callback) {
		this._client.del(key, callback);
	}

	smembers(key, callback) {
		this._client.smembers(key, callback);
	}

	sadd(key, value, callback) {
		this._client.sadd(key, value, callback);
	}

	srem(key, value, callback) {
		this._client.srem(key, value, callback);
	}

	hgetall(key, callback) {
		this._client.hgetall(key, callback);
	}

	scanDel(pattern, callback) {
		const self = this;
		this.scan(pattern, (keys, callback) => {
			self.emit('info', {message: 'redis#scanDel', data: {keys}});
			self._client.del(keys, callback);
		}, callback);
	}

	scan(pattern, delegate, callback) {

		const self = this;

		let cursor = '0';
		let cycle = 0;

		_scan(callback);

		function _scan(callback) {
			//noinspection JSUnresolvedFunction
			self._client.scan(
				cursor,
				'MATCH', pattern,
				'COUNT', SCAN_LIMIT,
				(err, res) => {

					if (err) {
						return callback(err);
					}

					cursor = res[0];
					cycle++;

					const keys = res[1];

					if (keys.length > 0) {
						delegate(keys, (err) => {
							if (err) {
								callback(err);
							} else {
								self.emit('info', {message: 'redis#scan', data: {cycle, scanned: SCAN_LIMIT * cycle}});
								processCycle();
							}
						});
					} else {
						self.emit('info', {message: 'redis#scan', data: {cycle, scanned: SCAN_LIMIT * cycle}});
						processCycle();
					}

					function processCycle() {
						if (cursor === '0') {
							callback();
						} else {
							_scan(callback);
						}
					}
				}
			);
		}
	}

}

export default Redis;