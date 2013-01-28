--redis keys
local closedHash 		= KEYS[1]
local processingHash	= KEYS[2]
local completedKey 		= KEYS[3]

--parameters
local task 				= ARGV[1]
local timestamp			= tonumber(ARGV[2])

redis.call('hdel', closedHash, task)
redis.call('hdel', processingHash, task)

redis.call('incr', completedKey)
