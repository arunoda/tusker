var ScriptManager   = require('./scriptManager');
var EventEmitter    = require('events').EventEmitter;
var util            = require('util');
var redis           = require('redis');
var debug           = require('debug')('tusker');

function Tusker(commandClient, watchingClient) {

    var scriptManager = new ScriptManager(commandClient);

    this.lock = function startSubTask (taskName, lockName, callback) {
        
        var timestamp = Date.now();
        commandClient.hset(getLockHashName(taskName), lockName, timestamp, callback);
    };

    this.unlock = function subTaskCompleted(taskName, lockName, callback) {

        var keys = [getLockHashName(taskName), getClosedHashName(), getReleasedListName()];
        var args = [taskName, lockName];
        scriptManager.run('unlock', keys, args, callback);
    };

    this.close = function close(taskName, info, options, callback) {

        info = info || {};
        
        if(typeof(options) == 'function') {
            callback = options;
            options = {};
        } else if (options == null) {
            options = {};
        }

        var metadata = {

            task: taskName,
            attempts: options.attempts || 5,
            timeout: options.timeout || 30 * 1000 //30 seconds
        };

        var data = {
            payload: info,
            metadata: metadata
        };

        var keys = [getLockHashName(taskName), getClosedHashName(), getReleasedListName()];
        var args = [taskName, JSON.stringify(data)];

        scriptManager.run('close', keys, args, callback);
    };

    this.fetchReleased = function watchForReleased(callback) {

        startWatching(callback);
    };

    this.timeoutLocks = function timeoutLocks(millis, callback) {

        var lockHashPrefix = getLockHashName('');
        var keys = [getClosedHashName(), getReleasedListName()];
        var args = [millis, Date.now(), lockHashPrefix]

        scriptManager.run('timeout-locks', keys, args, callback);
    };

    this.timeoutProcessing = function timeoutProcessing(callback) {

        var keys = [getClosedHashName(), getProcessingHashName(), getReleasedListName(), getFailedListName()];
        var args = [Date.now()]

        scriptManager.run('timeout-processing', keys, args, callback);
    };

    this.stats = function stats(callback) {

        var keys = [getClosedHashName(), getProcessingHashName(), getReleasedListName(), getFailedListName(), getCompletedKeyName()];
        var args = [];
        scriptManager.run('stats', keys, args, function(err, result) {

            if(err) {
                callback(err);
            } else {
                callback(null, JSON.parse(result));
            }
        });
    };

    function startWatching (callback) {
        
        var info;

        watchingClient.blpop(getReleasedListName(), 0, function(err, result) {

            debug('getting a task to process: ' + result);

            if(err) {
                callback(err);
            } if(!result) { 
                callback(new Error("Internal Error - Invalid Task Info"));
            } else {
                info = JSON.parse(result[1]);

                //notify redis as processing
                var keys = [getClosedHashName(), getProcessingHashName()];
                var args = [info.metadata.task, Date.now()];
                scriptManager.run('processing', keys, args, afterProcessingNotified);
            }
        });


        function afterProcessingNotified (err) {
            
            if(err) {
                callback(err);
            } else {
                callback(null, info.metadata.task, info.payload, processCompleted);
            }
        }

        function processCompleted(err, callback) {

            if(typeof(err) == 'function') {
                callback = err;
                err = null;
            }

            //error notify redis with the info
            if(err) {
                var keys = [getClosedHashName(), getProcessingHashName(), getReleasedListName(), getFailedListName()];
                var args = [info.metadata.task, Date.now()];
                scriptManager.run('failed', keys, args, callback);
            } else {
                var keys = [getClosedHashName(), getProcessingHashName(), getCompletedKeyName()];
                var args = [info.metadata.task, Date.now()];
                scriptManager.run('completed', keys, args, callback);
            }
        }
    }
}

util.inherits(Tusker, EventEmitter);

function getLockHashName (task) {
    
    return 'lock:' + task;
}

function getClosedHashName() {

    return 'closed';
}

function getReleasedListName() {

    return 'released';
}

function getProcessingHashName() {

    return 'processing';
}

function getFailedListName () {
    
    return 'failed';
}

function getCompletedKeyName() {

    return 'completed';
}

exports.initialize = function initialize() {

    var commandClient = exports.createClient();
    var watchingClient = exports.createClient();

    return new Tusker(commandClient, watchingClient);
};

exports.createClient = function createClient() {

    return redis.createClient();
};

exports.Tusker = Tusker;
exports._getLockHashName = getLockHashName;
exports._getClosedHashName = getClosedHashName;
exports._getReleasedListName = getReleasedListName;
exports._getProcessingHashName = getProcessingHashName;
exports._getFailedListName = getFailedListName;
exports._getCompletedKeyName = getCompletedKeyName;