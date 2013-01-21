var ScriptManager 	= require('./scriptManager');
var EventEmitter	= require('events').EventEmitter;
var util			= require('util');
var redis 			= require('redis');

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

	this.close = function close(taskName, info, callback) {

		info = info || {};
		info['task'] = taskName;

		var keys = [getLockHashName(taskName), getClosedHashName(), getReleasedListName()];
		var args = [taskName, JSON.stringify(info)];

		scriptManager.run('close', keys, args, callback);
	};

	this.watchForReleased = function watchForReleased(callback) {

		startWatching(callback);
	};

	function startWatching (callback) {
		
		watchingClient.blpop(getReleasedListName(), 0, function(err, result) {

			if(err) {
				callback(err);
			} else {

				var info = JSON.parse(result[1]);
				var taskName = info.task;

				delete info.task;

				callback(null, taskName, info, watchAnother);
			}
		});

		function watchAnother() {

			startWatching(callback);
		}
	}
}

util.inherits(Tusker, EventEmitter);

function getLockHashName (task) {
	
	return 'lock-' + task;
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