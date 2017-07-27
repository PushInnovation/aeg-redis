import * as redis from 'redis';
import { EventEmitter } from 'events';
import * as BBPromise from 'bluebird';
import Transaction from './transaction';
import Batch from './batch';
import { IRedisKeyOptions } from './types/redis';

BBPromise.promisifyAll(redis.RedisClient.prototype);
BBPromise.promisifyAll(redis.Multi.prototype);

export interface IRedisOptions {
	host: string;
	port: number;
	prefix?: string;
}

const SCAN_LIMIT = 1000;

class Redis extends EventEmitter {

	private _client: any;

	private _prefix: string;

	/**
	 * Constructor
	 */
	constructor (options: IRedisOptions) {

		super();

		this._prefix = options.prefix || '';
		this._client = redis.createClient(options);

	}

	/**
	 * Get the underlying redis client
	 */
	get client (): any {

		return this._client;

	}

	/**
	 * Close the client
	 */
	public async dispose (): Promise<void> {

		await this._client.quitAsync();

		this._client = null;

	}

	/**
	 * Return the transaction prototype
	 */
	static get Transaction (): typeof Transaction {

		return Transaction;

	}

	/**
	 * Get a transaction
	 */
	public transaction (): Transaction {

		this._checkDisposed();

		return new Transaction(this);

	}

	/**
	 * Watch a key
	 */
	public async watch (key: string): Promise<void> {

		this._checkDisposed();

		return this._client.watchAsync(key);

	}

	/**
	 * Begin a batch of commands
	 */
	public batch (): Batch {

		this._checkDisposed();

		return new Batch(this);

	}

	/**
	 * Key exists
	 */
	public async exists (key: string): Promise<boolean> {

		this._checkDisposed();

		return this._client.existsAsync(key);

	}

	/**
	 * Get a key value
	 */
	public async get (key: string): Promise<string> {

		this._checkDisposed();

		return this._client.getAsync(key);

	}

	/**
	 * Set a key value
	 */
	public async set (key: string, value: string | number, options?: IRedisKeyOptions): Promise<void> {

		this._checkDisposed();

		await this._client.setAsync(key, value);

		return this._checkExpiry(key, options);

	}

	/**
	 * Increment an integer value
	 */
	public async incrby (key: string, value: number, options?: IRedisKeyOptions): Promise<void> {

		this._checkDisposed();

		await this._client.incrbyAsync(key, value);

		return this._checkExpiry(key, options);

	}

	/**
	 * Increment a float value
	 */
	public async incrbyfloat (key: string, value: number, options?: IRedisKeyOptions): Promise<void> {

		this._checkDisposed();

		await this._client.incrbyfloatAsync(key, value);

		return this._checkExpiry(key, options);

	}

	/**
	 * Increment an integer value in a hash set
	 */
	public async hincrby (key: string, hashKey: string, value: number, options?: IRedisKeyOptions): Promise<void> {

		this._checkDisposed();

		await this._client.hincrbyAsync(key, hashKey, value);

		return this._checkExpiry(key, options);

	}

	/**
	 * Increment a float value in a hash set
	 */
	public async hincrbyfloat (key: string, hashKey: string, value: number, options?: IRedisKeyOptions): Promise<void> {

		this._checkDisposed();

		await this._client.hincrbyfloatAsync(key, hashKey, value);

		return this._checkExpiry(key, options);

	}

	/**
	 * Hash map set
	 */
	public async hmset (key: string, hash: object, options?: IRedisKeyOptions): Promise<void> {

		this._checkDisposed();

		await this._client.hmsetAsync(key, hash);

		return this._checkExpiry(key, options);

	}

	/**
	 * Get a hash set
	 */
	public async hgetall<T> (key: string): Promise<T> {

		this._checkDisposed();

		return this._client.hgetallAsync(key);

	}

	/**
	 * Delete a key
	 */
	public async del (key: string): Promise<void> {

		this._checkDisposed();

		return this._client.delAsync(key);

	}

	/**
	 * Add to a set
	 */
	public async sadd (
		key: string,
		value: string | number | Array<string | number>,
		options?: IRedisKeyOptions): Promise<void> {

		this._checkDisposed();

		await this._client.saddAsync(key, value);

		return this._checkExpiry(key, options);

	}

	/**
	 * Get a set
	 */
	public async smembers (key): Promise<string[]> {

		this._checkDisposed();

		return this._client.smembersAsync(key);

	}

	/**
	 * Remove from set
	 */
	public async srem (key: string, value: string | number): Promise<void> {

		this._checkDisposed();

		return this._client.sremAsync(key, value);

	}

	/**
	 * Scan and delete keys
	 */
	public async scanDel (pattern: string): Promise<void> {

		this._checkDisposed();

		const self = this;

		return this.scan(pattern, async (keys) => {

			self.emit('info', {message: 'redis#scanDel', data: {keys}});

			const keysToDel = keys.map((key) => {

				return key.replace(this._prefix, '');

			});

			return self._client.delAsync(keysToDel);

		});

	}

	/**
	 * Scan keys
	 */
	public async scan (pattern: string, delegate: (keys: string[]) => Promise<void> | void): Promise<void> {

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
	 */
	public async ttl (key): Promise<number> {

		this._checkDisposed();

		return this._client.ttlAsync(key);

	}

	/**
	 * Set the expiry in seconds
	 */
	private async _checkExpiry (key, options): Promise<void> {

		if (options && options.expire) {

			return this._client.expireAsync(key, options.expire);

		}

	}

	/**
	 * Check to make sure this transaction is still open
	 */
	private _checkDisposed (): void {

		if (!this._client) {

			throw new Error('Client disposed');

		}

	}

}

export default Redis;
