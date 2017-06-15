import redis from 'redis';
import Promise from 'bluebird';

Promise.promisifyAll(redis.RedisClient.prototype);
Promise.promisifyAll(redis.Multi.prototype);

class Transaction {

	/**
	 * Constructor
	 * @param {Object} client
	 */
	constructor (client) {

		this._client = client;
		this._multi = client._client.multi();

	}

	/* transactions */

	/**
	 * Commit a transaction
	 */
	async commit () {

		this._checkDisposed();

		const result = await this._multi.execAsync();

		this.rollback();

		if (!result) {

			throw new Error('Transaction failed');

		}

	}

	/**
	 * Roll the transaction back
	 */
	rollback () {

		this._checkDisposed();

		this._multi.discard();
		this._multi = null;

	}

	/**
	 * Is a transaction still open
	 */
	get isOpen () {

		return !!this._multi;

	}

	/* keys */

	/**
	 * Set a key value
	 * @param {string} key
	 * @param {string | number} value
	 * @param {Object} [options]
	 */
	async set (key, value, options) {

		this._checkDisposed();

		this._multi.set(key, value);

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

		this._multi.incrby(key, value);

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

		this._multi.incrbyfloat(key, value);

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

		this._multi.hincrby(key, hashKey, value);

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

		this._multi.hincrbyfloat(key, hashKey, value);

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

		this._multi.hmset(key, hash);

		return this._checkExpiry(key, options);

	}

	/**
	 * Delete a key
	 * @param {string} key
	 * @return {*}
	 */
	async del (key) {

		this._checkDisposed();

		this._multi.del(key);

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

		this._multi.sadd(key, value);

		return this._checkExpiry(key, options);

	}

	/**
	 * Remove from set
	 * @param {string} key
	 * @param {string | number} value
	 * @return {*}
	 */
	async srem (key, value) {

		this._checkDisposed();

		this._multi.srem(key, value);

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

			this._multi.expire(key, options.expire);

		}

	}

	/**
	 * Check to make sure this transaction is still open
	 * @returns null
	 * @private
	 */
	_checkDisposed () {

		if (!this._multi) {

			throw new Error('Transaction disposed');

		}

	}

}

export default Transaction;
