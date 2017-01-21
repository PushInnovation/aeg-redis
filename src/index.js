import redis from 'redis';
import { EventEmitter } from 'events';
import Promise from 'bluebird';
import _ from 'lodash';

Promise.promisifyAll(redis.RedisClient.prototype);
Promise.promisifyAll(redis.Multi.prototype);

const SCAN_LIMIT = 1000;

class Redis extends EventEmitter {

	/**
	 * Constructor
	 * @param {Object} [options]
	 */
	constructor (options) {

		options = options || {};

		super();
		this._prefix = options.prefix || '';
		this._client = redis.createClient(options);

	}

	/* transactions */

	/**
	 * Begin a transaction
	 */
	begin () {

		if (!this._multi) {

			this._multi = this._client.multi();

		} else {

			throw new Error('Transaction already open');

		}

	}

	/**
	 * Watch a key
	 * @param {string} key
	 * @returns {Promise.<*>}
	 */
	async watch (key) {

		return this._client.watchAsync(key);

	}

	/**
	 * Commit a transaction
	 */
	async commit () {

		if (!this._multi) {

			throw new Error('No transaction open');

		}

		const result = await this._multi.execAsync();

		this._multi = null;

		if (!result) {

			throw new Error('Transaction failed, please retry');

		}

	}

	/**
	 * Roll the transaction back
	 */
	rollback () {

		if (this._multi) {

			this._multi.discard();
			this._multi = null;

		} else {

			throw new Error('No transaction open');

		}

	}

	/* standard keys */

	/**
	 * Key exists
	 * @param {string} key
	 */
	async exists (key) {

		return this._client.existsAsync(key);

	}

	/**
	 * Get a key value
	 * @param {string} key
	 */
	async get (key) {

		return this._client.getAsync(key);

	}

	/**
	 * Set a key value
	 * @param {string} key
	 * @param {string | number} value
	 * @param {Object} [options]
	 */
	async set (key, value, options) {

		if (this._multi) {

			this._multi.set(key, value);

		} else {

			await this._client.setAsync(key, value);

		}

		return this._checkExpiry(key, options);

	}

	/**
	 * Increment an integer value
	 * @param {string} key
	 * @param {number} value
	 * @param {Object} [options]
	 * @return {*}
	 */
	async incrby (key, value, options) {

		if (this._multi) {

			this._multi.incrby(key, value);

		} else {

			await this._client.incrbyAsync(key, value);

		}

		return this._checkExpiry(key, options);

	}

	/**
	 * Increment a float value
	 * @param {string} key
	 * @param {number} value
	 * @param {Object} [options]
	 * @return {*}
	 */
	async incrbyfloat (key, value, options) {

		if (this._multi) {

			this._multi.incrbyfloat(key, value);

		} else {

			await this._client.incrbyfloatAsync(key, value);

		}

		return this._checkExpiry(key, options);

	}

	/**
	 * Increment an integer value in a hash set
	 * @param {string} key
	 * @param {string} hashKey
	 * @param {number} value
	 * @param {Object} [options]
	 * @return {*}
	 */
	async hincrby (key, hashKey, value, options) {

		if (this._multi) {

			this._multi.hincrby(key, hashKey, value);

		} else {

			await this._client.hincrbyAsync(key, hashKey, value);

		}

		return this._checkExpiry(key, options);

	}

	/**
	 * Increment a float value in a hash set
	 * @param {string} key
	 * @param {string} hashKey
	 * @param {number} value
	 * @param {Object} [options]
	 * @return {*}
	 */
	async hincrbyfloat (key, hashKey, value, options) {

		if (this._multi) {

			this._multi.hincrbyfloat(key, hashKey, value);

		} else {

			await this._client.hincrbyfloatAsync(key, hashKey, value);

		}

		return this._checkExpiry(key, options);

	}

	/**
	 * Hash map set
	 * @param {string} key
	 * @param {Object | *[] } hash
	 * @param {Object} [options]
	 * @return {*}
	 */
	async hmset (key, hash, options) {

		if (this._multi) {

			this._multi.hmset(key, hash);

		} else {

			await this._client.hmsetAsync(key, hash);

		}

		return this._checkExpiry(key, options);

	}

	/**
	 * Get a hash set
	 * @param {string} key
	 * @return {*}
	 */
	async hgetall (key) {

		return this._client.hgetallAsync(key);

	}

	/**
	 * Delete a key
	 * @param {string} key
	 * @return {*}
	 */
	async del (key) {

		if (this._multi) {

			this._multi.del(key);

		} else {

			return this._client.delAsync(key);

		}

	}

	/**
	 * Add to a set
	 * @param {string} key
	 * @param {string | number | *[]} value
	 * @param {Object} [options]
	 * @return {*}
	 */
	async sadd (key, value, options) {

		if (this._multi) {

			this._multi.sadd(key, value);

		} else {

			await this._client.saddAsync(key, value);

		}

		return this._checkExpiry(key, options);

	}

	/**
	 * Get a set
	 * @param {string} key
	 * @return {*}
	 */
	async smembers (key) {

		return this._client.smembersAsync(key);

	}

	/**
	 * Remove from set
	 * @param {string} key
	 * @param {string | number} value
	 * @return {*}
	 */
	async srem (key, value) {

		if (this._multi) {

			this._multi.srem(key, value);

		} else {

			return this._client.sremAsync(key, value);

		}

	}

	/**
	 * Scan and delete keys
	 * @param {string} pattern
	 * @return {*}
	 */
	async scanDel (pattern) {

		const self = this;

		return this.scan(pattern, async (keys) => {

			self.emit('info', {message: 'redis#scanDel', data: {keys}});

			if (self._multi) {

				self._multi.del(keys);

			} else {

				const keysToDel = _.map(keys, (key) => {

					return key.replace(this._prefix, '');

				});

				return self._client.delAsync(keysToDel);

			}

		});

	}

	/**
	 * Scan keys
	 * @param {string} pattern
	 * @param {function} delegate
	 */
	async scan (pattern, delegate) {

		const self = this;

		const resolvedPattern = this._prefix + pattern;

		let cursor = '0';
		let cycle = 0;

		return _scan();

		async function _scan () {

			const res = await self._client.scanAsync(
				cursor,
				'MATCH', resolvedPattern,
				'COUNT', SCAN_LIMIT);

			cursor = res[0];
			cycle++;

			const keys = res[1];

			if (keys.length > 0) {

				await delegate(keys);
				self.emit('info', {message: 'redis#scan', data: {cycle, scanned: SCAN_LIMIT * cycle}});
				await processCycle();

			} else {

				self.emit('info', {message: 'redis#scan', data: {cycle, scanned: SCAN_LIMIT * cycle}});
				await processCycle();

			}

			async function processCycle () {

				if (cursor !== '0') {

					await _scan();

				}

			}

		}

	}

	/**
	 * Get the remaining time of a key
	 * @param key
	 * @return {*}
	 */
	async ttl (key) {

		return this._client.ttlAsync(key);

	}

	/**
	 * Set the expiry in seconds
	 * @param {string} key
	 * @param {Object} [options]
	 * @return {*}
	 * @private
	 */
	async _checkExpiry (key, options) {

		if (options && options.expire) {

			if (this._multi) {

				this._multi.expire(key, options.expire);

			} else {

				return this._client.expireAsync(key, options.expire);

			}

		}

	}

}

export default Redis;
