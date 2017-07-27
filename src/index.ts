import Client from './client';
import Batch from './batch';
import Transaction from './transaction';
import { IRedisClient, IRedisBatch, IRedisTransaction, IRedisKeyOptions } from './types';

export default Client;

export {
	Client,
	Batch,
	Transaction,
	IRedisClient,
	IRedisBatch,
	IRedisTransaction,
	IRedisKeyOptions
};
