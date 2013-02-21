--redis keys
local taskHash  		= KEYS[1]
local closedHash 		= KEYS[2]
local releasedList 		= KEYS[3]
local processingHash 	= KEYS[4]

--parameters
local taskName 		= ARGV[1]
local closeInfo		= ARGV[2]

local noOfLocks	= redis.call('hlen', taskHash);

if noOfLocks == 0 then

	local isProcessing = redis.call('hget', processingHash, taskName)
	if not(isProcessing)  then
		--release the task
		redis.call('del', taskHash);
		redis.call('rpush', releasedList, closeInfo)
		redis.call('hset', closedHash, taskName, closeInfo)
	end
else
	--just add closeInfo
	redis.call('hset', closedHash, taskName, closeInfo)
end