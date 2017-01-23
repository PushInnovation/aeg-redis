import should from 'should';
import Client from '../../src/client';
import Promise from 'bluebird';

const client = new Client({
	host: '192.168.99.100',
	port: 32769
}).on('info', (obj) => console.log(obj));

before(async () => {

	return client.scanDel('test*');

});

after(async () => {

	await client.scanDel('test*');
	await client.dispose();

});

describe('#index()', async () => {

	describe('no transaction', async () => {

		it('should set a key value', async () => {

			await client.set('test1', 1);
			const val = await client.get('test1');
			should.exist(val);
			val.should.be.equal('1');

		});

		it('key should exist', async () => {

			const result = await client.exists('test1');
			result.should.be.ok;

		});

		it('should increment it by 1', async () => {

			await client.incrby('test1', 1);
			const val = await client.get('test1');
			should.exist(val);
			val.should.be.equal('2');

		});

		it('should increment it by 1', async () => {

			await client.incrbyfloat('test1', 1.1);
			const val = await client.get('test1');
			should.exist(val);
			val.should.be.equal('3.1');

		});

		it('should remove a key value', async () => {

			await client.del('test1');
			const val = await client.get('test1');
			should.not.exist(val);

		});

		it('key should not exist', async () => {

			const result = await client.exists('test1');
			result.should.not.be.ok;

		});

		it('should set a hash key value', async () => {

			await client.hmset('test1', {test: 1});
			const val = await client.hgetall('test1');
			should.exist(val);
			should(val).be.an.Object;
			should(val).have.property('test');
			val.test.should.be.equal('1');

		});

		it('should increment a hash key value', async () => {

			await client.hincrby('test1', 'test', 1);
			const val = await client.hgetall('test1');
			should.exist(val);
			should(val).be.an.Object;
			should(val).have.property('test');
			val.test.should.be.equal('2');

		});

		it('should increment a hash key value by float', async () => {

			await client.hincrbyfloat('test1', 'test', 1.1);
			const val = await client.hgetall('test1');
			should.exist(val);
			should(val).be.an.Object;
			should(val).have.property('test');
			val.test.should.be.equal('3.1');

		});

		it('should remove a key value', async () => {

			await client.del('test1');
			const val = await client.get('test1');
			should.not.exist(val);

		});

		it('should set a hash key value', async () => {

			await client.hmset('test2', ['test', 1]);
			const val = await client.hgetall('test2');
			should.exist(val);
			should(val).be.an.Object;
			should(val).have.property('test');
			val.test.should.be.equal('1');

		});

		it('should remove a key value', async () => {

			await client.del('test2');
			const val = await client.get('test2');
			should.not.exist(val);

		});

		it('should set a set key value by array', async () => {

			await client.sadd('test1', [1, 2, 3]);
			const val = await client.smembers('test1');
			should.exist(val);
			should(val).be.an.Array;
			val.length.should.be.equal(3);

		});

		it('should set a set key value', async () => {

			await client.sadd('test1', 4);
			const val = await client.smembers('test1');
			should.exist(val);
			should(val).be.an.Array;
			val.length.should.be.equal(4);

		});

		it('should delete a set key value', async () => {

			await client.srem('test1', 1);
			const val = await client.smembers('test1');
			should.exist(val);
			should(val).be.an.Array;
			val.length.should.be.equal(3);

		});

		it('should remove a key value', async () => {

			await client.del('test1');
			const val = await client.get('test1');
			should.not.exist(val);

		});

		it('should set a 2001 key values', async () => {

			await Promise.map([...new Array(2001)].map((_, i) => i), (val) => {

				return client.set('test' + val, val);

			});

		});

		it('should set a 100 key values', async () => {

			let counter = 0;

			await client.scan('test*', (res) => {

				counter += res.length;

			});

			counter.should.be.equal(2001);

		});

		it('should remove a 2001 key values', async () => {

			let counter = 0;

			await client.scanDel('test*');

			await client.scan('test*', (res) => {

				counter += res.length;

			});

			counter.should.be.equal(0);

		});

	});

	describe('expiry no transaction', async () => {

		it('should set a key value', async () => {

			await client.set('test1', 1, {expire: 30});
			await testExpire();

		});

		it('should increment it by 1', async () => {

			await client.incrby('test1', 1, {expire: 30});
			await testExpire();

		});

		it('should increment it by 1', async () => {

			await client.incrbyfloat('test1', 1.1, {expire: 30});
			await testExpire();

		});

		it('should set a hash key value', async () => {

			await client.hmset('test1', {test: 1}, {expire: 30});
			await testExpire();

		});

		it('should increment a hash key value', async () => {

			await client.hincrby('test1', 'test', 1, {expire: 30});
			await testExpire();

		});

		it('should increment a hash key value by float', async () => {

			await client.hincrbyfloat('test1', 'test', 1.1, {expire: 30});
			await testExpire();

		});

		it('should set a hash key value', async () => {

			await client.sadd('test1', 4, {expire: 30});
			await testExpire();

		});

	});

	describe('transactions', async () => {

		it('should begin and rollback', () => {

			const transaction = client.transaction();
			transaction.isOpen.should.be.ok;
			transaction.rollback();

		});

		it('should set a key value', async () => {

			await testTransaction('test1', async (transaction) => {

				await transaction.set('test1', 1);

			}, async () => {

				const val = await client.get('test1');
				val.should.be.equal('1');

			});

		});

		it('should increment a key value', async () => {

			await testTransaction('test1', async (transaction) => {

				await transaction.incrby('test1', 1);

			}, async () => {

				const val = await client.get('test1');
				val.should.be.equal('1');

			});

		});

		it('should increment a key value by float', async () => {

			await testTransaction('test1', async (transaction) => {

				await transaction.incrbyfloat('test1', 1);

			}, async () => {

				const val = await client.get('test1');
				val.should.be.equal('1');

			});

		});

		it('should create a hash set', async () => {

			await testTransaction('test1', async (transaction) => {

				await transaction.hmset('test1', {test: 1});

			}, async () => {

				const val = await client.hgetall('test1');
				val.test.should.be.equal('1');

			});

		});

		it('should create and increment hash set', async () => {

			await testTransaction('test1', async (transaction) => {

				await transaction.hmset('test1', {test: 1});
				await transaction.hincrby('test1', 'test', 1);

			}, async () => {

				const val = await client.hgetall('test1');
				val.test.should.be.equal('2');

			});

		});

		it('should create and increment hash set by float', async () => {

			await testTransaction('test1', async (transaction) => {

				await transaction.hmset('test1', {test: 1});
				await transaction.hincrbyfloat('test1', 'test', 1.1);

			}, async () => {

				const val = await client.hgetall('test1');
				val.test.should.be.equal('2.1');

			});

		});

		it('should create a set', async () => {

			await testTransaction('test1', async (transaction) => {

				await transaction.sadd('test1', [1, 2]);

			}, async () => {

				const val = await client.smembers('test1');
				val.length.should.be.equal(2);
				val[0].should.be.equal('1');

			});

		});

		it('should remove a set member', async () => {

			await testTransaction('test1', async (transaction) => {

				await transaction.sadd('test1', [1, 2]);
				await transaction.srem('test1', 1);

			}, async () => {

				const val = await client.smembers('test1');
				val[0].should.be.equal('2');

			});

		});

	});

	describe('expiry transaction', async () => {

		it('should set a key value', async () => {

			await testExpireTransaction(async (transaction) => {

				await transaction.set('test1', 1, {expire: 30});

			});

		});

		it('should increment it by 1', async () => {

			await testExpireTransaction(async (transaction) => {

				await transaction.incrby('test1', 1, {expire: 30});

			});

		});

		it('should increment it by 1', async () => {

			await testExpireTransaction(async (transaction) => {

				await transaction.incrbyfloat('test1', 1.1, {expire: 30});

			});

		});

		it('should set a hash key value', async () => {

			await testExpireTransaction(async (transaction) => {

				await transaction.hmset('test1', {test: 1}, {expire: 30});

			});

		});

		it('should increment a hash key value', async () => {

			await testExpireTransaction(async (transaction) => {

				await transaction.hincrby('test1', 'test', 1, {expire: 30});

			});

		});

		it('should increment a hash key value by float', async () => {

			await testExpireTransaction(async (transaction) => {

				await transaction.hincrbyfloat('test1', 'test', 1.1, {expire: 30});

			});

		});

		it('should set a hash key value', async () => {

			await testExpireTransaction(async (transaction) => {

				await transaction.sadd('test1', 4, {expire: 30});

			});

		});

	});

	describe('transactions with watches', async () => {

		it('should conflict', async () => {

			const client2 = new Client({
				host: '192.168.99.100',
				port: 32769
			}).on('info', (obj) => console.log(obj));

			await client2.watch('test-c');
			const transaction = client2.transaction();

			await client.set('test-c', 4);
			await transaction.set('test-c', 3);

			try {

				await transaction.commit();
				should.fail('Should not have commited transaction');

			} catch (ex) {

				if (ex instanceof should.AssertionError) {

					throw ex;

				}

			}

		});

		it('should not conflict', async () => {

			const client2 = new Client({
				host: '192.168.99.100',
				port: 32769
			}).on('info', (obj) => console.log(obj));

			await client.set('test-c', 5);

			await client2.watch('test-c');

			const transaction = client2.transaction();
			await transaction.set('test-c', 3);
			await transaction.commit();

		});

	});

});

async function testTransaction (key, delegate, check) {

	let transaction = client.transaction();
	await delegate(transaction);
	const result1 = await client.exists(key);
	result1.should.not.be.ok;
	await transaction.commit();
	const result2 = await client.exists(key);
	result2.should.be.ok;
	await check();
	transaction = client.transaction();
	await transaction.del(key);
	const result3 = await client.exists(key);
	result3.should.be.ok;
	await transaction.commit();
	const result4 = await client.exists(key);
	result4.should.not.be.ok;

}

async function testExpire () {

	const ttl = await client.ttl('test1');
	(ttl <= 30 && ttl >= 1).should.be.ok;
	await client.del('test1');

}

async function testExpireTransaction (delegate) {

	const transaction = client.transaction();
	await delegate(transaction);
	await transaction.commit();
	const ttl = await client.ttl('test1');
	(ttl <= 30 && ttl >= 1).should.be.ok;
	await client.del('test1');

}
