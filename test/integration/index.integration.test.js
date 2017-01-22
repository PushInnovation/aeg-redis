import should from 'should';
import Redis from '../../src/index';
import Promise from 'bluebird';

const redis = new Redis({
	host: '192.168.99.100',
	port: 32769
}).on('info', (obj) => console.log(obj));

before(async () => {

	return redis.scanDel('test*');

});

after(async () => {

	return redis.scanDel('test*');

});

describe('#index()', async () => {

	describe('no transaction', async () => {

		it('should set a key value', async () => {

			redis.isTransactionOpen.should.be.false;

		});

		it('should set a key value', async () => {

			await redis.set('test1', 1);
			const val = await redis.get('test1');
			should.exist(val);
			val.should.be.equal('1');

		});

		it('key should exist', async () => {

			const result = await redis.exists('test1');
			result.should.be.ok;

		});

		it('should increment it by 1', async () => {

			await redis.incrby('test1', 1);
			const val = await redis.get('test1');
			should.exist(val);
			val.should.be.equal('2');

		});

		it('should increment it by 1', async () => {

			await redis.incrbyfloat('test1', 1.1);
			const val = await redis.get('test1');
			should.exist(val);
			val.should.be.equal('3.1');

		});

		it('should remove a key value', async () => {

			await redis.del('test1');
			const val = await redis.get('test1');
			should.not.exist(val);

		});

		it('key should not exist', async () => {

			const result = await redis.exists('test1');
			result.should.not.be.ok;

		});

		it('should set a hash key value', async () => {

			await redis.hmset('test1', {test: 1});
			const val = await redis.hgetall('test1');
			should.exist(val);
			should(val).be.an.Object;
			should(val).have.property('test');
			val.test.should.be.equal('1');

		});

		it('should increment a hash key value', async () => {

			await redis.hincrby('test1', 'test', 1);
			const val = await redis.hgetall('test1');
			should.exist(val);
			should(val).be.an.Object;
			should(val).have.property('test');
			val.test.should.be.equal('2');

		});

		it('should increment a hash key value by float', async () => {

			await redis.hincrbyfloat('test1', 'test', 1.1);
			const val = await redis.hgetall('test1');
			should.exist(val);
			should(val).be.an.Object;
			should(val).have.property('test');
			val.test.should.be.equal('3.1');

		});

		it('should remove a key value', async () => {

			await redis.del('test1');
			const val = await redis.get('test1');
			should.not.exist(val);

		});

		it('should set a hash key value', async () => {

			await redis.hmset('test2', ['test', 1]);
			const val = await redis.hgetall('test2');
			should.exist(val);
			should(val).be.an.Object;
			should(val).have.property('test');
			val.test.should.be.equal('1');

		});

		it('should remove a key value', async () => {

			await redis.del('test2');
			const val = await redis.get('test2');
			should.not.exist(val);

		});

		it('should set a set key value by array', async () => {

			await redis.sadd('test1', [1, 2, 3]);
			const val = await redis.smembers('test1');
			should.exist(val);
			should(val).be.an.Array;
			val.length.should.be.equal(3);

		});

		it('should set a set key value', async () => {

			await redis.sadd('test1', 4);
			const val = await redis.smembers('test1');
			should.exist(val);
			should(val).be.an.Array;
			val.length.should.be.equal(4);

		});

		it('should delete a set key value', async () => {

			await redis.srem('test1', 1);
			const val = await redis.smembers('test1');
			should.exist(val);
			should(val).be.an.Array;
			val.length.should.be.equal(3);

		});

		it('should remove a key value', async () => {

			await redis.del('test1');
			const val = await redis.get('test1');
			should.not.exist(val);

		});

		it('should set a 2001 key values', async () => {

			await Promise.map([...new Array(2001)].map((_, i) => i), (val) => {

				return redis.set('test' + val, val);

			});

		});

		it('should set a 100 key values', async () => {

			let counter = 0;

			await redis.scan('test*', (res) => {

				counter += res.length;

			});

			counter.should.be.equal(2001);

		});

		it('should remove a 2001 key values', async () => {

			let counter = 0;

			await redis.scanDel('test*');

			await redis.scan('test*', (res) => {

				counter += res.length;

			});

			counter.should.be.equal(0);

		});

	});

	describe('expiry no transaction', async () => {

		it('should set a key value', async () => {

			await redis.set('test1', 1, {expire: 30});
			await testExpire();

		});

		it('should increment it by 1', async () => {

			await redis.incrby('test1', 1, {expire: 30});
			await testExpire();

		});

		it('should increment it by 1', async () => {

			await redis.incrbyfloat('test1', 1.1, {expire: 30});
			await testExpire();

		});

		it('should set a hash key value', async () => {

			await redis.hmset('test1', {test: 1}, {expire: 30});
			await testExpire();

		});

		it('should increment a hash key value', async () => {

			await redis.hincrby('test1', 'test', 1, {expire: 30});
			await testExpire();

		});

		it('should increment a hash key value by float', async () => {

			await redis.hincrbyfloat('test1', 'test', 1.1, {expire: 30});
			await testExpire();

		});

		it('should set a hash key value', async () => {

			await redis.sadd('test1', 4, {expire: 30});
			await testExpire();

		});

	});

	describe('transactions', async () => {

		it('should begin and rollback', () => {

			redis.begin();
			redis.isTransactionOpen.should.be.ok;
			redis.rollback();

		});

		it('should set a key value', async () => {

			await testTransaction('test1', async () => {

				await redis.set('test1', 1);

			}, async () => {

				const val = await redis.get('test1');
				val.should.be.equal('1');

			});

		});

		it('should increment a key value', async () => {

			await testTransaction('test1', async () => {

				await redis.incrby('test1', 1);

			}, async () => {

				const val = await redis.get('test1');
				val.should.be.equal('1');

			});

		});

		it('should increment a key value by float', async () => {

			await testTransaction('test1', async () => {

				await redis.incrbyfloat('test1', 1);

			}, async () => {

				const val = await redis.get('test1');
				val.should.be.equal('1');

			});

		});

		it('should create a hash set', async () => {

			await testTransaction('test1', async () => {

				await redis.hmset('test1', {test: 1});

			}, async () => {

				const val = await redis.hgetall('test1');
				val.test.should.be.equal('1');

			});

		});

		it('should create and increment hash set', async () => {

			await testTransaction('test1', async () => {

				await redis.hmset('test1', {test: 1});
				await redis.hincrby('test1', 'test', 1);

			}, async () => {

				const val = await redis.hgetall('test1');
				val.test.should.be.equal('2');

			});

		});

		it('should create and increment hash set by float', async () => {

			await testTransaction('test1', async () => {

				await redis.hmset('test1', {test: 1});
				await redis.hincrbyfloat('test1', 'test', 1.1);

			}, async () => {

				const val = await redis.hgetall('test1');
				val.test.should.be.equal('2.1');

			});

		});

		it('should create a set', async () => {

			await testTransaction('test1', async () => {

				await redis.sadd('test1', [1, 2]);

			}, async () => {

				const val = await redis.smembers('test1');
				val.length.should.be.equal(2);
				val[0].should.be.equal('1');

			});

		});

		it('should remove a set member', async () => {

			await testTransaction('test1', async () => {

				await redis.sadd('test1', [1, 2]);
				await redis.srem('test1', 1);

			}, async () => {

				const val = await redis.smembers('test1');
				val[0].should.be.equal('2');

			});

		});

		it('should scanDel', async () => {

			await Promise.map([...new Array(2001)].map((_, i) => i), (val) => {

				return redis.set('test' + val, val);

			});

			let counter = 0;

			redis.begin();

			await redis.scanDel('test*');

			await redis.scan('test*', (res) => {

				counter += res.length;

			});

			counter.should.be.equal(2001);

			counter = 0;

			await redis.commit();

			await redis.scan('test*', (res) => {

				counter += res.length;

			});

			counter.should.be.equal(0);

		});

	});

	describe('expiry transaction', async () => {

		it('should set a key value', async () => {

			await testExpireTransaction(async () => {

				await redis.set('test1', 1, {expire: 30});

			});

		});

		it('should increment it by 1', async () => {

			await testExpireTransaction(async () => {

				await redis.incrby('test1', 1, {expire: 30});

			});

		});

		it('should increment it by 1', async () => {

			await testExpireTransaction(async () => {

				await redis.incrbyfloat('test1', 1.1, {expire: 30});

			});

		});

		it('should set a hash key value', async () => {

			await testExpireTransaction(async () => {

				await redis.hmset('test1', {test: 1}, {expire: 30});

			});

		});

		it('should increment a hash key value', async () => {

			await testExpireTransaction(async () => {

				await redis.hincrby('test1', 'test', 1, {expire: 30});

			});

		});

		it('should increment a hash key value by float', async () => {

			await testExpireTransaction(async () => {

				await redis.hincrbyfloat('test1', 'test', 1.1, {expire: 30});

			});

		});

		it('should set a hash key value', async () => {

			await testExpireTransaction(async () => {

				await redis.sadd('test1', 4, {expire: 30});

			});

		});

	});

	describe('transactions with watches', async () => {

		it('should conflict', async () => {

			const redis2 = new Redis({
				host: '192.168.99.100',
				port: 32769
			}).on('info', (obj) => console.log(obj));

			await redis2.watch('test-c');
			redis2.begin();

			await redis.set('test-c', 4);
			await redis2.set('test-c', 3);

			try {

				await redis2.commit();
				should.fail('Should not have commited transaction');

			} catch (ex) {

				if (ex instanceof should.AssertionError) {

					throw ex;

				}

			}

		});

		it('should not conflict', async () => {

			const redis2 = new Redis({
				host: '192.168.99.100',
				port: 32769
			}).on('info', (obj) => console.log(obj));

			await redis.set('test-c', 5);

			await redis2.watch('test-c');

			redis2.begin();
			await redis2.set('test-c', 3);
			await redis2.commit();

		});

	});

});

async function testTransaction (key, delegate, check) {

	redis.begin();
	await delegate();
	const result1 = await redis.exists(key);
	result1.should.not.be.ok;
	await redis.commit();
	const result2 = await redis.exists(key);
	result2.should.be.ok;
	await check();
	redis.begin();
	await redis.del(key);
	const result3 = await redis.exists(key);
	result3.should.be.ok;
	await redis.commit();
	const result4 = await redis.exists(key);
	result4.should.not.be.ok;

}

async function testExpire () {

	const ttl = await redis.ttl('test1');
	(ttl <= 30 && ttl >= 1).should.be.ok;
	await redis.del('test1');

}

async function testExpireTransaction (delegate) {

	redis.begin();
	await delegate();
	await redis.commit();
	const ttl = await redis.ttl('test1');
	(ttl <= 30 && ttl >= 1).should.be.ok;
	await redis.del('test1');

}
