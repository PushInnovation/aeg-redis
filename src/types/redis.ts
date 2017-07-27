export interface IRedisKeyOptions {
	expire: number;
}

export interface IRedisTransaction {
	commit: () => Promise<void>;
	rollback: () => void;
	readonly isOpen: boolean;
}

export interface IRedisBatch {
	exec (): Promise<any[]>;
}
