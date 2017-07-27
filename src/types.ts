export interface IRedisKeyOptions {
	expire?: number;
}

export interface IRedisClient {
	readonly disposed: boolean;
}

export interface IRedisTransaction {
	readonly disposed: boolean;
	commit: () => Promise<void>;
	rollback: () => void;
}

export interface IRedisBatch {
	readonly disposed: boolean;
	exec (): Promise<any[]>;
}
