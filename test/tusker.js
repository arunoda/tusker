var assert      = require('assert');
var tusker      = require('../lib/tusker');
var Tusker      = tusker.Tusker;
var redis       = require('redis');

var redisClient = redis.createClient();
var redisClient2 = redis.createClient();

suite('Tusker', function() {

    suite('.lock()', function() {

        test('add a lock', _clean(function(done) {

            var task = 'the-task';
            var lockName = 'lock-here';

            var t = new Tusker(redisClient);
            t.lock(task, lockName, function(err) {

                assert.equal(err, undefined);
                redisClient.hget(tusker._getLockHashName(task), lockName, verifyAddLock);
            });

            function verifyAddLock (err, result) {
                
                assert.equal(err, null);
                result = parseInt(result);

                assert.ok(result > 0 && result <= Date.now());
                done();
            }
        }));
    });

    suite('.unlock()', function() {

        test('task not closed', _clean(function(done) {

            var task = 'the-task';
            var lockName = 'lock-here';

            var t = new Tusker(redisClient);
            t.lock(task, lockName, function(err) {

                assert.equal(err, undefined);
                t.unlock(task, lockName, veryifyTaskUnlock);
            });

            function veryifyTaskUnlock (err) {
                
                if(err) throw err;
                redisClient.hget(tusker._getLockHashName(task), lockName, function(err, result) {

                    assert.equal(err, undefined);
                    assert.equal(result, null);
                    done();
                });
            }
        }));

        test('task already closed and no other locks', _clean(function(done) {
        
            var task = 'the-task';
            var lockName = 'lock-here';
            var info = 'the-info';

            var t = new Tusker(redisClient);
            t.lock(task, lockName, function(err) {

                assert.equal(err, undefined);
                //close the task
                redisClient.hset(tusker._getClosedHashName(), task, info, unlockTask);
            });

            function unlockTask (err) {
                
                assert.equal(err, undefined);
                t.unlock(task, lockName, function(err) {

                    assert.equal(err, undefined);
                    redisClient.hget(tusker._getClosedHashName(), task, verifyCloseHashExist);
                });
            }

            function verifyCloseHashExist(err, result) {

                assert.equal(err, undefined);
                assert.equal(result, null);

                redisClient.lpop(tusker._getReleasedListName(), function(err, result) {

                    assert.equal(err, null);
                    assert.equal(result, info);
                    done();
                });
            }

        }));

        test('task already closed and another lock(s) exists', _clean(function(done) {
            
            var task = 'the-task';
            var lockName = 'lock-here';
            var info = 'the-info';

            var t = new Tusker(redisClient);
            t.lock(task, lockName, function(err) {

                assert.equal(err, undefined);
                t.lock(task, 'some-other-lock', function(err) {

                    assert.equal(err, undefined);
                    //close the task
                    redisClient.hset(tusker._getClosedHashName(), task, info, unlockTask);
                });
            });

            function unlockTask (err) {
                
                assert.equal(err, undefined);
                t.unlock(task, lockName, function(err) {

                    assert.equal(err, undefined);
                    redisClient.hget(tusker._getClosedHashName(), task, verifyCloseHashExist);
                });
            }

            function verifyCloseHashExist(err, result) {

                assert.equal(err, undefined);
                assert.equal(result, info);

                redisClient.lpop(tusker._getReleasedListName(), function(err, result) {

                    assert.equal(err, null);
                    assert.equal(result, null);
                    done();
                });
            }
        }));
    });

    suite('.close()', function() {

        test('no locks exists', _clean(function(done) {

            var taskName = 'the-task';
            var info = { data: 10 };
            var t = new Tusker(redisClient);

            t.close(taskName, info, function(err) {

                assert.equal(err, undefined);
                redisClient.hget(tusker._getClosedHashName(), taskName, verifyClosedHash);
            });
            
            function verifyClosedHash(err, result) {

                assert.equal(err, undefined);
                assert.equal(result, null);

                redisClient.lpop(tusker._getReleasedListName(), veryifyReleased);
            }

            function veryifyReleased(err, result) {

                assert.equal(err, undefined);
                result = JSON.parse(result);

                assert.equal(result.metadata.task, taskName);
                assert.equal(result.payload.data, info.data);

                done();
            }
        }));

        test('one or more locks exists', _clean(function(done) {

            var taskName = 'the-task';
            var lockName = 'lock-here';
            var info = { data: 10 };
            var t = new Tusker(redisClient);

            t.lock(taskName, lockName, function(err) {

                assert.equal(err, undefined);
                t.close(taskName, info, verifyTaskClosed);
            });

            function verifyTaskClosed(err) {

                assert.equal(err, undefined);
                redisClient.hget(tusker._getClosedHashName(), taskName, function(err, result) {

                    assert.equal(err, undefined);
                    result = JSON.parse(result);

                    assert.equal(result.metadata.task, taskName);
                    assert.equal(result.payload.data, info.data);

                    redisClient.lpop(tusker._getReleasedListName(), veryifyNotReleased);
                });
            }

            function veryifyNotReleased(err, result) {

                assert.equal(err, undefined);
                assert.equal(result, null);

                done();
            }

        }));

    });

    suite('.fetchReleased()', function() {

        test('watching after closed', _clean(function(done) {

            var task = 'the-task';
            var lockName = 'lock-here';
            var info = { data: 100 };

            var t = new Tusker(redisClient, redisClient2);
            t.fetchReleased(function(err, _task, _info) {

                assert.equal(err, null);
                assert.equal(_info.data, info.data);
                assert.equal(_task, task);
                done();
            });

            t.lock(task, lockName, function(err) {

                assert.equal(err, undefined);
                t.close(task, info, unlockTask);
            });

            function unlockTask(err) {

                assert.equal(err, undefined);
                t.unlock(task, lockName, function(err) {

                    assert.equal(err, undefined);
                });
            }

        }));
    });

    suite('.initialize()', function() {

        test('watching after closed using initialize()', _clean(function(done) {

            var task = 'the-task';
            var lockName = 'lock-here';
            var info = { data: 100 };

            var clientsCreated = 0;
            tusker.createClient = function() {

                clientsCreated++;
                return redis.createClient();
            };

            var t = tusker.initialize();
            t.fetchReleased(function(err, _task, _info) {

                assert.equal(err, null);
                assert.equal(_info.data, info.data);
                assert.equal(_task, task);

                assert.equal(clientsCreated, 2);
                done();
            });

            t.lock(task, lockName, function(err) {

                assert.equal(err, undefined);
                t.close(task, info, unlockTask);
            });

            function unlockTask(err) {

                assert.equal(err, undefined);
                t.unlock(task, lockName, function(err) {

                    assert.equal(err, undefined);
                });
            }

        }));
    });

    suite('.timeoutLocks()', function() {

        test("2 timeouted locks exists in a single lockHash and closed", _clean(function(done) {

            var task = 'abc-test';
            var timeoutMillis = 30 * 1000;
            var millis = Date.now() - (timeoutMillis + 2000);
            var info = {data: 242}

            var t = new Tusker(redisClient);

            redisClient.hmset(tusker._getLockHashName(task), 'lock1', millis, 'lock2', millis, function(err) {

                assert.equal(err, undefined);
                t.close(task, info, function(err) {

                    assert.equal(err, undefined);
                    t.timeoutLocks(timeoutMillis, veryfyTimeout);
                });
            });

            function veryfyTimeout(err) {

                if(err) throw err;
                redisClient.hmget(tusker._getLockHashName(task), 'lock1', 'lock2', function(err, result) {

                    assert.equal(err, null);
                    assert.deepEqual(result, [null, null]);

                    redisClient.lpop(tusker._getReleasedListName(), afterReleasedFound);
                });
            }

            function afterReleasedFound(err, result) {

                result = JSON.parse(result);

                assert.equal(err, null);
                assert.equal(result.payload.data, info.data);
                done();
            };
        }));

        test('2 timeout locks and 1 active lock exists in a single lockHash and closed', _clean(function(done) {

            var task = 'abc-test';
            var timeoutMillis = 30 * 1000;
            var millis = Date.now() - (timeoutMillis + 2000);
            var timestamp = Date.now();
            var info = {data: 242}

            var t = new Tusker(redisClient);

            redisClient.hmset(tusker._getLockHashName(task), 'lock1', millis, 'lock2', millis, 'lock3', timestamp, function(err) {

                assert.equal(err, undefined);
                t.close(task, info, function(err) {

                    assert.equal(err, undefined);
                    t.timeoutLocks(timeoutMillis, veryfyTimeout);
                });
            });

            function veryfyTimeout(err) {

                if(err) throw err;
                redisClient.hmget(tusker._getLockHashName(task), 'lock1', 'lock2', 'lock3', function(err, result) {

                    assert.equal(err, null);
                    assert.deepEqual(result, [null, null, timestamp]);

                    redisClient.lpop(tusker._getReleasedListName(), afterReleasedFound);
                });
            }

            function afterReleasedFound(err, result) {
                
                assert.equal(err, null);
                assert.equal(result, null);
                done();
            };
        }));
    });
});

function _clean(callback) {

    return function(done) {

        redisClient.flushdb(function(err) {

            if(err) throw err;
            callback(done);
        });
    };
}