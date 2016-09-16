import should from 'should';
import Redis from '../../src/index';
import Promise from 'bluebird';

const redis = new Redis({
	host: '192.168.99.100',
	port: 32769
}).on('info', (obj) => console.log(obj));

describe('#index()', async () => {

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

	it('should set a hash key value by array', async () => {

		await redis.sadd('test1', [1, 2, 3]);
		const val = await redis.smembers('test1');
		should.exist(val);
		should(val).be.an.Array;
		val.length.should.be.equal(3);

	});

	it('should set a hash key value', async () => {

		await redis.sadd('test1', 4);
		const val = await redis.smembers('test1');
		should.exist(val);
		should(val).be.an.Array;
		val.length.should.be.equal(4);

	});

	it('should delete a hash key value', async () => {

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
