--redis keys
local closedHash 		= KEYS[1]
local processingHash	= KEYS[2]

--parameters
local task 				= ARGV[1]
local timestamp			= tonumber(ARGV[2])

local closeInfo = redis.call('hget', closedHash, task)
local closeInfoJson = cjson.decode(closeInfo)

closeInfoJson['metadata']['started'] = timestamp

redis.call('hset', processingHash, task, cjson.encode(closeInfoJson))
