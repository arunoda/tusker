--redis keys
local taskHash  	= KEYS[1]
local closedHash 	= KEYS[2]
local releasedList 	= KEYS[3]

--parameters
local taskName 		= ARGV[1]
local lockName		= ARGV[2]

redis.call('hdel', taskHash, lockName)

local noOfLocks = redis.call('hlen', taskHash)
local closeInfo = redis.call('hget', closedHash, taskName)

if noOfLocks == 0 and closeInfo ~= nil then
	--trigger released if the task already closed and no more locks
	redis.call('del', taskHash);
	redis.call('rpush', releasedList, closeInfo)
end
