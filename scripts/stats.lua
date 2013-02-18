--redis keys
local closedHash 		= KEYS[1]
local processingHash 	= KEYS[2]
local releasedList 		= KEYS[3]
local failedList 		= KEYS[4]
local completedKey 		= KEYS[5]


local sizeClosed = redis.call('hlen', closedHash)
local sizeProcessing = redis.call('hlen', processingHash)
local sizeReleased = redis.call('llen', releasedList)
local sizeFailed = redis.call('llen', failedList)
local sizeCompleted = tonumber(redis.call('get', completedKey)) or 0

local result = {
	failed=sizeFailed,
	completed=sizeCompleted,
	released=sizeReleased,
	processing=sizeProcessing,
	locked=sizeClosed - sizeProcessing - sizeReleased
}

return cjson.encode(result)