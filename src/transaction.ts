import Client from './client';
import { IRedisKeyOptions, IRedisTransaction } from './types/redis';

export default class Transaction implements IRedisTransaction {

	private _client: Client;

	private _multi: any | null;

	/**
	 * Constructor
	 */
	constructor (client: Client) {

		this._client = client;
		this._multi = client.client.multi();

	}

	/**
	 * Commit a transaction
	 */
	public async commit (): Promise<void> {

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
	public rollback (): void {

		this._checkDisposed();

		this._multi.discard();
		this._multi = null;

	}

	/**
	 * Is a transaction still open
	 */
	public get isOpen (): boolean {

		return !!this._multi;

	}

	/**
	 * Set a key value
	 */
	public async set (key: string, value: string | number, options?: IRedisKeyOptions): Promise<void> {

		this._checkDisposed();

		this._multi.set(key, value);

		return this._checkExpiry(key, options);

	}

	/**
	 * Increment an integer value
	 */
	public async incrby (key: string, value: number, options?: IRedisKeyOptions): Promise<void> {

		this._checkDisposed();

		this._multi.incrby(key, value);

		return this._checkExpiry(key, options);

	}

	/**
	 * Increment a float value
	 */
	public async incrbyfloat (key: string, value: number, options?: IRedisKeyOptions): Promise<void> {

		this._checkDisposed();

		this._multi.incrbyfloat(key, value);

		return this._checkExpiry(key, options);

	}

	/**
	 * Increment an integer value in a hash set
	 */
	public async hincrby (key: string, hashKey: string, value: number, options?: IRedisKeyOptions): Promise<void> {

		this._checkDisposed();

		this._multi.hincrby(key, hashKey, value);

		return this._checkExpiry(key, options);

	}

	/**
	 * Increment a float value in a hash set
	 */
	public async hincrbyfloat (key: string, hashKey: string, value: number, options?: IRedisKeyOptions) {

		this._checkDisposed();

		this._multi.hincrbyfloat(key, hashKey, value);

		return this._checkExpiry(key, options);

	}

	/**
	 * Hash map set
	 */
	public async hmset (key: string, hash: object, options?: IRedisKeyOptions): Promise<void> {

		this._checkDisposed();

		this._multi.hmset(key, hash);

		return this._checkExpiry(key, options);

	}

	/**
	 * Delete a key
	 */
	public async del (key: string): Promise<void> {

		this._checkDisposed();

		this._multi.del(key);

	}

	/**
	 * Add to a set
	 */
	public async sadd (key: string, value: string | number, options?: IRedisKeyOptions): Promise<void> {

		this._checkDisposed();

		this._multi.sadd(key, value);

		return this._checkExpiry(key, options);

	}

	/**
	 * Remove from set
	 */
	public async srem (key: string, value: string | number): Promise<void> {

		this._checkDisposed();

		this._multi.srem(key, value);

	}

	/**
	 * Set the expiry in seconds
	 */
	private async _checkExpiry (key: string, options?: IRedisKeyOptions): Promise<void> {

		if (options && options.expire) {

			this._multi.expire(key, options.expire);

		}

	}

	/**
	 * Check to make sure this transaction is still open
	 */
	private _checkDisposed (): void {

		if (!this._multi) {

			throw new Error('Transaction disposed');

		}

	}

}
