--redis keys
local closedHash 		= KEYS[1]
local processingHash	= KEYS[2]
local releasedList	 	= KEYS[3]
local failedList		= KEYS[4]

--parameters
local task 				= ARGV[1]
local timestamp			= tonumber(ARGV[2])

redis.call('hdel', processingHash, task)

local closeInfo = redis.call('hget', closedHash, task)
local closeInfoJson = cjson.decode(closeInfo)

closeInfoJson['metadata']['attempts'] = closeInfoJson['metadata']['attempts'] - 1;
if closeInfoJson['metadata']['attempts'] > 0 then
	--add to released list again
	closeInfoJson['metadata']['started'] = nil
	redis.call('rpush', releasedList, cjson.encode(closeInfoJson))

	redis.call('hset', closedHash, task, cjson.encode(closeInfoJson))
else
	--add to failed list
	redis.call('hdel', closedHash, task)

	local taskInfo = {task=task, failed=timestamp}
	redis.call('rpush', failedList, cjson.encode(taskInfo))
end

