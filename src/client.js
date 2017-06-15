import redis from 'redis';
import { EventEmitter } from 'events';
import Promise from 'bluebird';
import _ from 'lodash';
import Transaction from './transaction';
import Batch from './batch';

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
		this._batch = null;

	}

	/**
	 * Close the client
	 * @returns {Promise.<*>}
	 */
	async dispose () {

		await this._client.quitAsync();

		this._client = null;

	}

	/* transactions */

	/**
	 * Return the transaction prototype
	 * @returns {Transaction}
	 * @constructor
	 */
	static get Transaction () {

		return Transaction;

	}

	/**
	 * Get a transaction
	 * @returns {Transaction}
	 */
	transaction () {

		this._checkDisposed();

		return new Transaction(this);

	}

	/**
	 * Watch a key
	 * @param {string} key
	 * @returns {Promise.<*>}
	 */
	async watch (key) {

		this._checkDisposed();

		return this._client.watchAsync(key);

	}

	/* batch */

	/** Begin a batch of commands **/
	batch () {

		this._checkDisposed();

		return new Batch(this);

	}

	/* keys */

	/**
	 * Key exists
	 * @param {string} key
	 */
	async exists (key) {

		this._checkDisposed();

		return this._client.existsAsync(key);

	}

	/**
	 * Get a key value
	 * @param {string} key
	 */
	async get (key) {

		this._checkDisposed();

		return this._client.getAsync(key);

	}

	/**
	 * Set a key value
	 * @param {string} key
	 * @param {string | number} value
	 * @param {Object} [options]
	 */
	async set (key, value, options) {

		this._checkDisposed();

		await this._client.setAsync(key, value);

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

		this._checkDisposed();

		await this._client.incrbyAsync(key, value);

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

		this._checkDisposed();

		await this._client.incrbyfloatAsync(key, value);

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

		this._checkDisposed();

		await this._client.hincrbyAsync(key, hashKey, value);

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

		this._checkDisposed();

		await this._client.hincrbyfloatAsync(key, hashKey, value);

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

		this._checkDisposed();

		await this._client.hmsetAsync(key, hash);

		return this._checkExpiry(key, options);

	}

	/**
	 * Get a hash set
	 * @param {string} key
	 * @return {*}
	 */
	async hgetall (key) {

		this._checkDisposed();

		return this._client.hgetallAsync(key);

	}

	/**
	 * Delete a key
	 * @param {string} key
	 * @return {*}
	 */
	async del (key) {

		this._checkDisposed();

		return this._client.delAsync(key);

	}

	/**
	 * Add to a set
	 * @param {string} key
	 * @param {string | number | *[]} value
	 * @param {Object} [options]
	 * @return {*}
	 */
	async sadd (key, value, options) {

		this._checkDisposed();

		await this._client.saddAsync(key, value);

		return this._checkExpiry(key, options);

	}

	/**
	 * Get a set
	 * @param {string} key
	 * @return {*}
	 */
	async smembers (key) {

		this._checkDisposed();

		return this._client.smembersAsync(key);

	}

	/**
	 * Remove from set
	 * @param {string} key
	 * @param {string | number} value
	 * @return {*}
	 */
	async srem (key, value) {

		this._checkDisposed();

		return this._client.sremAsync(key, value);

	}

	/**
	 * Scan and delete keys
	 * @param {string} pattern
	 * @return {*}
	 */
	async scanDel (pattern) {

		this._checkDisposed();

		const self = this;

		return this.scan(pattern, async (keys) => {

			self.emit('info', {message: 'redis#scanDel', data: {keys}});

			const keysToDel = _.map(keys, (key) => {

				return key.replace(this._prefix, '');

			});

			return self._client.delAsync(keysToDel);

		});

	}

	/**
	 * Scan keys
	 * @param {string} pattern
	 * @param {function} delegate
	 */
	async scan (pattern, delegate) {

		this._checkDisposed();

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

		this._checkDisposed();

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

			return this._client.expireAsync(key, options.expire);

		}

	}

	/**
	 * Check to make sure this transaction is still open
	 * @returns null
	 * @private
	 */
	_checkDisposed () {

		if (!this._client) {

			throw new Error('Client disposed');

		}

	}

}

export default Redis;
