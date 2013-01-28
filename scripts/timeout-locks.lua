--redis keys
local closedHash 		= KEYS[1]
local releasedList 		= KEYS[2]

--parameters
local timeoutMillis 	= tonumber(ARGV[1])
local timestamp			= tonumber(ARGV[2])
local lockHaskPrefix	= ARGV[3]

local taskHashes = redis.call('keys', lockHaskPrefix .. '*');

--iterate over all lock hashes
for index, taskHash in ipairs(taskHashes) do

	local taskName = taskHash:gsub(lockHaskPrefix, "")
	local locks = redis.call('hgetall', taskHash)

	--remove any locks get timeouted
	for i= 1, #locks, 2 do

		local lockName = locks[i]
		local lockCreatedAt = tonumber(locks[i + 1])

		if (lockCreatedAt + timeoutMillis) < timestamp then
			redis.call('hdel', taskHash, lockName)
		end
	end

	--check for closed
	local closeInfo = redis.call('hget', closedHash, taskName)
	local noOfLocks = redis.call('hlen', taskHash)

	if closeInfo and noOfLocks == 0 then
		--trigger released if the task already closed and no more locks
		redis.call('del', taskHash);
		redis.call('rpush', releasedList, closeInfo)
	end
end