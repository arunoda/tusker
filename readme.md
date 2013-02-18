[![Build Status](https://travis-ci.org/arunoda/tusker.png?branch=master)](https://travis-ci.org/arunoda/tusker)
# Tusker - Redis Based Distributed Task Locking

## Why? Let's look at following scenario

* We receive chunks of ogg's to encode into different servers
* All of those are related to single stream
* And we also receive the end of the stream too
* Now we need to notify back, after all the chunks are encoded

## Do it with Tusker

### When we are encoding chunks
~~~js
var tusker = require('tusker');
var taskManager = tusker.initialize();

//when we are begin to encode a chunk
var lockname = "" + Math.random();
taskManager.lock('the-task', lockname);

//after we do the encoding
taskManager.unlock('the-task', lockname);
~~~

### When we are closing the stream

~~~js
var tusker = require('tusker');
var taskManager = tusker.initialize();

var info = { some_data: 10 };
var options = {}
taskManager.close('the-task', info, options);
~~~

#### Available options

##### attempts
* no of attempts before before mark the task as failed
* new attempt can be occurred if task completed with an error or timeout occurred

##### timeout
* timeout for the task completion in millis
* if this value exceeded new attempt will be occurred


### Receive notification after every chunk is encoded

~~~js
var tusker = require('tusker');
var taskManager = tusker.initialize();

taskManager.fetchReleased(function(err, taskName, info, completed) {

    //merge chunks and notify back
    var err = null; //if there is an error assign error object 
    completed(err);
});
~~~

### Invoking a timeout for locks

We cannot assure every lock we create will be unlocked. If the server died before `unlock`, we have a lock which will never be closed. So we need a timeout machanism.

~~~js
var tusker = require('tusker
var taskManager = tusker.initialize();

//one minute timeout
taskManager.timeoutLocks(60 * 1000, function(err) {
    
});
~~~

>NOTE: We've to invoke this method via a `cron` like tool

### Invoking a timeout for task processing

* We need to timeout long running tasks, and allow another attempt to process it
* default timeout is 30 seconds otherwise if it is not defined when task closing via `.close()` 

~~~js
var tusker = require('tusker');
var taskManager = tusker.initialize();

//one minute timeout
taskManager.timeoutProcessing(function(err) {
    
});
~~~

>NOTE: We've to invoke this method via a `cron` like tool


### Statistics
~~~js
var tusker = require('tusker');
var taskManager = tusker.initialize();

//one minute timeout
taskManager.stats(function(err, stats) {
    

});
~~~

Stats object is formatted as shown below:

	{ completed: 101, released: 1, locked: 2, failed: 2, processing: 2 }
