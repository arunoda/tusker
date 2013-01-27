var ScriptManager   = require('./scriptManager');
var EventEmitter    = require('events').EventEmitter;
var util            = require('util');
var redis           = require('redis');

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

    this.timeout = function timeout(millis, callback) {

        var lockHashPrefix = getLockHashName('');
        var keys = [getClosedHashName(), getReleasedListName()];
        var args = [millis, Date.now(), lockHashPrefix]

        scriptManager.run('timeout', keys, args, callback);
    };

    function startWatching (callback) {
        
        watchingClient.blpop(getReleasedListName(), 0, function(err, result) {

            if(err) {
                callback(err);
            } else {

                var info = JSON.parse(result[1]);
                var taskName = info.metadata.task;

                callback(null, taskName, info.payload, processCompleted);
            }
        });

        function processCompleted(err) {

            //error notify redis with the info
        }
    }
}

util.inherits(Tusker, EventEmitter);

function getLockHashName (task) {
    
    return 'lock:' + task;
}

function getClosedHashName() {

    return 'closed';
};

function getReleasedListName() {

    return 'released';
};

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