import Redis from '../../src/index';

const redis = new Redis({
	host: '192.168.99.100',
	port: 32769
});

describe('#smembers()', () => {

	it('should return empty array', (done) => {

		redis.smembers('nokey', (err, result) => {

			result.should.be.an.Array;
			result.length.should.be.equal(0);
			done(err);

		});

	});

});
