var assert      = require('assert');
var tusker      = require('../lib/tusker');
var Tusker      = tusker.Tusker;
var redis       = require('redis');

var redisClient = redis.createClient();
var redisClient2 = redis.createClient();

redisClient.setMaxListeners(50);

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
                assert.equal(result, info);

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
                result = JSON.parse(result);
                assert.equal(result.payload.data, info.data);

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

        test('watching after closed - completed successfully', _clean(function(done) {

            var task = 'the-task';
            var lockName = 'lock-here';
            var info = { data: 100 };

            var t = new Tusker(redisClient, redisClient2);
            var completed;
            t.fetchReleased(function(err, _task, _info, _completed) {

                assert.equal(err, null);
                assert.equal(_info.data, info.data);
                assert.equal(_task, task);
                
                //verify processing
                completed = _completed;
                redisClient.hget(tusker._getProcessingHashName(), task, verifyProcessingHash);
            });

            function verifyProcessingHash(err, result) {

                assert.equal(err, undefined);
                result = JSON.parse(result);

                assert.equal(result.payload.data, info.data);
                assert.ok(result.metadata.started > 0 && result.metadata.started <= Date.now());

                completed(verifyCompleted);            
            }

            function verifyCompleted(err) {

                assert.equal(err, undefined);
                redisClient.multi()
                    .hget(tusker._getClosedHashName(), task)
                    .hget(tusker._getProcessingHashName(), task)
                    .get(tusker._getCompletedKeyName())
                    .exec(function(err, results) {

                        assert.equal(err, null);
                        assert.equal(results[0], null);
                        assert.equal(results[1], null);

                        assert.equal(results[2], 1);
                        done();
                    });
            }

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

        test('watching after closed - completed with a failure - has attempts left', _clean(function(done) {

            var task = 'the-task';
            var lockName = 'lock-here';
            var info = { data: 100 };

            var t = new Tusker(redisClient, redisClient2);
            var completed;
            t.fetchReleased(function(err, _task, _info, _completed) {

                assert.equal(err, null);
                assert.equal(_info.data, info.data);
                assert.equal(_task, task);
                
                //verify processing
                completed = _completed;
                redisClient.hget(tusker._getProcessingHashName(), task, verifyProcessingHash);
            });

            function verifyProcessingHash(err, result) {

                assert.equal(err, undefined);
                result = JSON.parse(result);

                assert.equal(result.payload.data, info.data);
                assert.ok(result.metadata.started > 0 && result.metadata.started <= Date.now());

                completed({code: 'ERROR'}, verifyCompleted);            
            }

            function verifyCompleted(err) {


                assert.equal(err, undefined);
                redisClient.multi()
                    .hget(tusker._getClosedHashName(), task)
                    .hget(tusker._getProcessingHashName(), task)
                    .lpop(tusker._getReleasedListName())
                    .exec(function(err, results) {

                        assert.equal(err, null);

                        results[0] = JSON.parse(results[0]);
                        assert.equal(results[0].payload.data, info.data);
                        assert.equal(results[0].metadata.attempts, 4);
                        assert.equal(results[1], null);

                        results[2] = JSON.parse(results[2]);
                        assert.equal(results[2].payload.data, info.data);
                        assert.equal(results[2].metadata.attempts, 4);

                        done();
                    });
            }

            t.lock(task, lockName, function(err) {

                assert.equal(err, undefined);
                t.close(task, info, {attempts: 5}, unlockTask);
            });

            function unlockTask(err) {

                assert.equal(err, undefined);
                t.unlock(task, lockName, function(err) {

                    assert.equal(err, undefined);
                });
            }

        }));

        test('watching after closed - completed with a failure - has no attempts left', _clean(function(done) {

            var task = 'the-task';
            var lockName = 'lock-here';
            var info = { data: 100 };

            var t = new Tusker(redisClient, redisClient2);
            var completed;
            t.fetchReleased(function(err, _task, _info, _completed) {

                assert.equal(err, null);
                assert.equal(_info.data, info.data);
                assert.equal(_task, task);
                
                //verify processing
                completed = _completed;
                redisClient.hget(tusker._getProcessingHashName(), task, verifyProcessingHash);
            });

            function verifyProcessingHash(err, result) {

                assert.equal(err, undefined);
                result = JSON.parse(result);

                assert.equal(result.payload.data, info.data);
                assert.ok(result.metadata.started > 0 && result.metadata.started <= Date.now());

                completed({code: 'ERROR'}, verifyCompleted);            
            }

            function verifyCompleted(err) {


                assert.equal(err, undefined);
                redisClient.multi()
                    .hget(tusker._getClosedHashName(), task)
                    .hget(tusker._getProcessingHashName(), task)
                    .lpop(tusker._getFailedListName())
                    .exec(function(err, results) {

                        assert.equal(err, null);

                        assert.equal(results[0], null);
                        assert.equal(results[1], null);

                        results[2] = JSON.parse(results[2]);
                        assert.equal(results[2].task, task);
                        assert.ok(results[2].failed > 0 && results[2].failed <= Date.now());

                        done();
                    });
            }

            t.lock(task, lockName, function(err) {

                assert.equal(err, undefined);
                t.close(task, info, {attempts: 1}, unlockTask);
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

    suite('.timeoutProcessing()', function() {

        test('remove 2 timeouted tasks with remaining attempts > 1', _clean(function(done) {

            var t = new Tusker(redisClient, redisClient2);     
            t.close('task1', {}, {timeout: 1});
            t.close('task2', {}, {timeout: 1000 * 100});
            t.close('task3', {}, {timeout: 1});
            t.close('task4', {}, {timeout: 1000 * 100});

            setTimeout(function() {

                for(var lc=0; lc<4; lc++) {
                    t.fetchReleased(function(err, task) {

                        assert.equal(err, null);
                        assert.ok(task);
                    });
                }

                setTimeout(function() {

                    t.timeoutProcessing(verifyTimeout);
                }, 10)
            }, 10);


            function verifyTimeout(err) {

                assert.equal(err, null);

                redisClient.multi()
                    .hgetall(tusker._getClosedHashName())
                    .hgetall(tusker._getProcessingHashName())
                    .lrange(tusker._getReleasedListName(), 0, -1)
                    .exec(function(err, results) {

                        assert.equal(err, null);

                        assert.equal(JSON.parse(results[0]['task1']).metadata.attempts, 4)
                        assert.equal(JSON.parse(results[0]['task2']).metadata.attempts, 5)
                        assert.equal(JSON.parse(results[0]['task3']).metadata.attempts, 4)
                        assert.equal(JSON.parse(results[0]['task4']).metadata.attempts, 5)

                        assert.equal(results[1]['task1'], null)
                        assert.equal(JSON.parse(results[1]['task2']).metadata.attempts, 5)
                        assert.equal(results[1]['task3'], null)
                        assert.equal(JSON.parse(results[1]['task4']).metadata.attempts, 5)

                        assert.ok(['task1', 'task3'].indexOf(JSON.parse(results[2][0]).metadata.task) >= 0);
                        assert.ok(['task1', 'task3'].indexOf(JSON.parse(results[2][1]).metadata.task) >= 0);

                        done();
                    });
            }

        }));

        test('remove 2 timeouted tasks with no remaining attempts', _clean(function(done) {

            var t = new Tusker(redisClient, redisClient2);     
            t.close('task1', {}, {timeout: 1, attempts: 1});
            t.close('task2', {}, {timeout: 1000 * 100});
            t.close('task3', {}, {timeout: 1, attempts: 1});
            t.close('task4', {}, {timeout: 1000 * 100});

            setTimeout(function() {

                for(var lc=0; lc<4; lc++) {
                    t.fetchReleased(function(err, task) {

                        assert.equal(err, null);
                        assert.ok(task);
                    });
                }

                setTimeout(function() {

                    t.timeoutProcessing(verifyTimeout);
                }, 10)
            }, 10);


            function verifyTimeout(err) {

                assert.equal(err, null);

                redisClient.multi()
                    .hgetall(tusker._getClosedHashName())
                    .hgetall(tusker._getProcessingHashName())
                    .lrange(tusker._getReleasedListName(), 0, -1)
                    .lrange(tusker._getFailedListName(), 0, -1)
                    .exec(function(err, results) {

                        assert.equal(err, null);

                        assert.equal(Object.keys(results[0]).length, 2)
                        assert.equal(JSON.parse(results[0]['task2']).metadata.attempts, 5)
                        assert.equal(JSON.parse(results[0]['task4']).metadata.attempts, 5)

                        assert.equal(Object.keys(results[1]).length, 2)
                        assert.equal(JSON.parse(results[1]['task2']).metadata.attempts, 5)
                        assert.equal(JSON.parse(results[1]['task4']).metadata.attempts, 5)

                        assert.equal(results[2].length, 0);

                        assert.ok(['task1', 'task3'].indexOf(JSON.parse(results[3][0]).task) >= 0);
                        assert.ok(['task1', 'task3'].indexOf(JSON.parse(results[3][1]).task) >= 0);

                        done();
                    });
            }

        }));
    })

    suite('.stats', function() {

        test('getting empty stats', _clean(function(done) {

            var t = new Tusker(redisClient);
            var expected = { completed: 0, released: 0, locked: 0, failed: 0, processing: 0 };

            t.stats(function(err, results) {

                assert.equal(err, null);
                assert.deepEqual(results, expected);
                done();
            });
        }));

        test('getting stats with simulated data', _clean(function(done) {

            var t = new Tusker(redisClient);
            redisClient.multi()
                .hmset(tusker._getClosedHashName(), 1, 10, 2, 20, 3, 30, 4, 40, 5, 50)
                .hmset(tusker._getProcessingHashName(), 1, 10, 2, 20)
                .rpush(tusker._getReleasedListName(), 10)
                .rpush(tusker._getFailedListName(), 1, 2)
                .set(tusker._getCompletedKeyName(), 101)
                .exec(function(err, results) {

                    assert.equal(err, null);
                    assert.deepEqual(results, ["OK", "OK", 1, 2, "OK"]);
                    t.stats(checkStats);
                });

            function checkStats(err, stats) {

                assert.equal(err, null);
                
                var expected = { completed: 101, released: 1, locked: 2, failed: 2, processing: 2 };
                assert.deepEqual(stats, expected);
                done();
            }   
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