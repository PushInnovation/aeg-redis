'use strict';

import redis from 'redis';

const SCAN_LIMIT = 1000;

class Redis {

	constructor(options) {
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
		this._client.scan(pattern, (keys, callback) => {
			self._client.del(keys, callback);
		}, callback);
	}

	scan(pattern, delegate, callback) {

		let cursor = '0';
		let cycle = 0;

		_scan(callback);

		function _scan(callback) {
			//noinspection JSUnresolvedFunction
			this._client.scan(
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
								processCycle();
							}
						});
					} else {
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