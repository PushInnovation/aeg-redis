import redis from 'redis';
import {EventEmitter} from 'events';

const SCAN_LIMIT = 1000;

class Redis extends EventEmitter {

	constructor (options) {

		super();
		this._client = redis.createClient(options);

	}

	/* transactions */

	// noinspection JSUnusedGlobalSymbols
	begin (callback) {

		if (!this._multi) {

			this._multi = this._client.multi();
			callback();

		} else {

			callback(new Error('Transaction already open'));

		}

	}

	// noinspection JSUnusedGlobalSymbols
	commit (callback) {

		if (this._multi) {

			this._multi.exec((err) => {

				this._multi = null;
				callback(err);

			});

		} else {

			callback(new Error('No transaction open'));

		}

	}

	// noinspection JSUnusedGlobalSymbols
	rollback (callback) {

		if (this._multi) {

			// noinspection JSUnresolvedFunction
			this._multi.discard();
			this._multi = null;
			callback();

		} else {

			callback(new Error('No transaction open'));

		}

	}

	/* standard keys */

	// noinspection JSUnusedGlobalSymbols
	exists (key, callback) {

		this._client.exists(key, callback);

	}

	// noinspection JSUnusedGlobalSymbols
	get (key, callback) {

		this._client.get(key, callback);

	}

	// noinspection JSUnusedGlobalSymbols
	set (key, value, options, callback) {

		let args = Array.prototype.slice.call(arguments);
		key = args.shift();
		value = args.shift();
		callback = args.pop();
		options = args.length > 0 ? args.shift() : null;

		if (this._multi) {

			this._multi.set(key, value);
			this._checkExpiry(key, options, callback);

		} else {

			this._client.set(key, value, callback);

		}

	}

	// noinspection JSUnusedGlobalSymbols
	incrby (key, value, options, callback) {

		let args = Array.prototype.slice.call(arguments);
		key = args.shift();
		value = args.shift();
		callback = args.pop();
		options = args.length > 0 ? args.shift() : null;

		if (this._multi) {

			this._multi.incrby(key, value);
			this._checkExpiry(key, options, callback);

		} else {

			this._client.incrby(key, value, callback);

		}

	}

	// noinspection JSUnusedGlobalSymbols
	incrbyfloat (key, value, options, callback) {

		let args = Array.prototype.slice.call(arguments);
		key = args.shift();
		value = args.shift();
		callback = args.pop();
		options = args.length > 0 ? args.shift() : null;

		if (this._multi) {

			this._multi.incrbyfloat(key, value);
			this._checkExpiry(key, options, callback);

		} else {

			this._client.incrbyfloat(key, value, callback);

		}

	}

	// noinspection JSUnusedGlobalSymbols
	hincrby (key, hashKey, value, options, callback) {

		let args = Array.prototype.slice.call(arguments);
		key = args.shift();
		hashKey = args.shift();
		value = args.shift();
		callback = args.pop();
		options = args.length > 0 ? args.shift() : null;

		if (this._multi) {

			this._multi.hincrby(key, hashKey, value);
			this._checkExpiry(key, options, callback);

		} else {

			this._client.hincrby(key, hashKey, value, callback);

		}

	}

	// noinspection JSUnusedGlobalSymbols
	hincrbyfloat (key, hashKey, value, options, callback) {

		let args = Array.prototype.slice.call(arguments);
		key = args.shift();
		hashKey = args.shift();
		value = args.shift();
		callback = args.pop();
		options = args.length > 0 ? args.shift() : null;

		if (this._multi) {

			this._multi.hincrbyfloat(key, hashKey, value);
			this._checkExpiry(key, options, callback);

		} else {

			this._client.hincrbyfloat(key, hashKey, value, callback);

		}

	}

	// noinspection JSUnusedGlobalSymbols
	hmset (key, hash, options, callback) {

		let args = Array.prototype.slice.call(arguments);
		key = args.shift();
		hash = args.shift();
		callback = args.pop();
		options = args.length > 0 ? args.shift() : null;

		if (this._multi) {

			this._multi.hmset(key, hash);
			this._checkExpiry(key, options, callback);

		} else {

			this._client.hmset(key, hash, callback);

		}

	}

	del (key, callback) {

		if (this._multi) {

			this._multi.del(key);
			callback();

		} else {

			this._client.del(key, callback);

		}

	}

	// noinspection JSUnusedGlobalSymbols
	smembers (key, callback) {

		this._client.smembers(key, callback);

	}

	// noinspection JSUnusedGlobalSymbols
	sadd (key, value, options, callback) {

		let args = Array.prototype.slice.call(arguments);
		key = args.shift();
		value = args.shift();
		callback = args.pop();
		options = args.length > 0 ? args.shift() : null;

		if (this._multi) {

			this._multi.sadd(key, value);
			this._checkExpiry(key, options, callback);

		} else {

			this._client.sadd(key, value, callback);

		}

	}

	// noinspection JSUnusedGlobalSymbols
	srem (key, value, callback) {

		if (this._multi) {

			this._multi.srem(key, value);
			callback();

		} else {

			this._client.srem(key, value, callback);

		}

	}

	// noinspection JSUnusedGlobalSymbols
	hgetall (key, callback) {

		this._client.hgetall(key, callback);

	}

	// noinspection JSUnusedGlobalSymbols
	scanDel (pattern, callback) {

		const self = this;
		this.scan(pattern, (keys, callback) => {

			self.emit('info', {message: 'redis#scanDel', data: {keys}});
			if (self._multi) {

				self._multi.del(keys);
				callback();

			} else {

				self._client.del(keys, callback);

			}

		}, callback);

	}

	scan (pattern, delegate, callback) {

		const self = this;

		let cursor = '0';
		let cycle = 0;

		_scan(callback);

		function _scan (callback) {

			// noinspection JSUnresolvedFunction
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

					function processCycle () {

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

	_checkExpiry (key, options, callback) {

		// noinspection JSUnresolvedVariable
		if (options && options.expire) {

			if (this._multi) {

				// noinspection JSUnresolvedVariable,JSUnresolvedFunction
				this._multi.expire(key, options.expire);
				callback();

			} else {

				// noinspection JSUnresolvedVariable,JSUnresolvedFunction
				this._client.expire(key, options.expire, callback);

			}

		}

	}

}

export default Redis;
