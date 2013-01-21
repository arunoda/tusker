#Tusker - Redis Based Distributed Task Locking

##Why? Let's look at following scenario

* We receive chunks of ogg's to encode into different servers
* All of those are related to single stream
* And we also receive the end of the stream too
* Now we need to notify back, after all the chunks are encoded


##Do it with Tusker

###When we are encoding chunks
~~~js
var tusker = require('tusker');
var taskManager = tusker.initialize();

//when we are begin to encode a chunk
var lockname = "" + Math.random();
taskManager.lock('the-task', lockname);

//after we do the encoding
taskManager.unlock('the-task', lockname);
~~~

###When we are closing the stream

~~~js
var tusker = require('tusker');
var taskManager = tusker.initialize();

var info = { some_data: 10 };
taskManager.close('the-task', info);
~~~

###Receive notification after every chunk is encoded

~~~js
taskManager.watchForReleased(function(err, taskName, info, watchAgain) {

	//merge chunks and notify back
	watchAgain();
});
~~~