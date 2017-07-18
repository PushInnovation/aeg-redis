import Client from './client';

export default class Batch {

	private _client: Client;

	private _batch: any | null;

	/**
	 * Constructor
	 */
	constructor (client: Client) {

		this._client = client;
		this._batch = client.client.batch();

	}

	/**
	 * Key exists
	 */
	public async exists (key: string): Promise<void> {

		this._checkDisposed();

		this._batch!.exists(key);

	}

	/**
	 * Get a key value
	 */
	public async get (key: string): Promise<void> {

		this._checkDisposed();

		this._batch!.get(key);

	}

	/**
	 * Get a hash set
	 */
	public async hgetall (key: string): Promise<void> {

		this._checkDisposed();

		this._batch!.hgetall(key);

	}

	/**
	 * Get a set
	 */
	public async smembers (key: string): Promise<void> {

		this._checkDisposed();

		this._batch!.smembers(key);

	}

	/**
	 * Execute a batch of commands
	 */
	public async exec (): Promise<any[]> {

		this._checkDisposed();

		try {

			return await new Promise<any[]>((resolve, reject) => {

				this._batch!.exec((err, res) => {

					if (err) {

						reject(err);

					} else {

						resolve(res);

					}

				});

			});

		} catch (ex) {

			throw ex;

		} finally {

			this._batch = null;

		}

	}

	/**
	 * Check to make sure this batch is still open
	 */
	private _checkDisposed (): void {

		if (!this._batch) {

			throw new Error('Batch disposed');

		}

	}

}
