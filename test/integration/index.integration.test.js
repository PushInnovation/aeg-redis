import should from 'should';
import Redis from '../../src/index';

const redis = new Redis({
	host: '192.168.99.100',
	port: 32769
});

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

	it('should remove a key value', async () => {

		await redis.del('test1');
		const val = await redis.get('test1');
		should.not.exist(val);

	});

	it('key should not exist', async () => {

		const result = await redis.exists('test1');
		result.should.not.be.ok;

	});

});
