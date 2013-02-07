var fs      = require('fs');
var path    = require('path');
var debug   = require('debug')('redis-lua');

var scriptsDir = path.resolve(path.dirname(__filename), '../scripts');
var SCRIPTS = loadScripts(scriptsDir);

function ScriptManager (redisClient) {
    
    var scriptShas = {};
    debug('loading script initially');
    loadScriptsIntoRedis(redisClient, SCRIPTS, afterShasLoaded);

    this.run = function run(scriptName, keys, args, callback) {

        if(SCRIPTS[scriptName]) {
            var args = [keys.length].concat(keys, args);
            
            if(scriptShas[scriptName]) {
                var sha = scriptShas[scriptName];
                args.unshift(sha);
                redisClient.send_command('evalsha', args, callback);
            } else {
                var script = SCRIPTS[scriptName];
                args.unshift(script);
                redisClient.send_command('eval', args, callback);
            }
        } else {
            callback({code: 'NO_SUCH_SCRIPT', message: "No such script named: " + scriptName});
        }
    };

    //load scripts into redis in every time it connects to it
    redisClient.on('connect', function() {

        debug('loading scripts into redis again, aftet-reconnect');
        loadScriptsIntoRedis(redisClient, SCRIPTS, afterShasLoaded);
    }); 

    //reset shas after error occured
    redisClient.on('error', function(err) {

        var errorMessage = (err)? err.toString() : "";
        debug('resetting scriptShas due to redis connection error: ' + errorMessage);
        scriptShas = {};
    });   

    function afterShasLoaded(err, shas) {

        if(err) {
            debug('resetting scriptShas due to redis command error: ' + err.toString());
            scriptShas = {};
        } else {
            debug('loaded scriptShas');
            scriptShas = shas;
        }
    }

}

module.exports = ScriptManager;

function loadScripts(scriptDir) {

    var names = fs.readdirSync(scriptsDir);
    var scripts = {};

    names.forEach(function(name) {

        var filename = path.resolve(scriptsDir, name);
        var key = name.replace('.lua', '');

        scripts[key] = fs.readFileSync(filename, 'utf8');
    });

    return scripts;
}

function loadScriptsIntoRedis (redisClient, scripts, callback) {
    
    var cnt = 0;
    var keys = Object.keys(scripts);
    var shas = {};

    (function doLoad() {

        if(cnt < keys.length) {
            var key = keys[cnt++];

            redisClient.send_command('script', ['load', scripts[key]], function(err, sha) {

                if(err) {
                    callback(err);
                } else {
                    shas[key] = sha;
                    doLoad();
                }
            });
        } else {
            callback(null, shas);
        }

    })();
}
