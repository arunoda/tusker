--redis keys
local closedHash 		= KEYS[1]
local processingHash	= KEYS[2]
local releasedList	 	= KEYS[3]
local failedList		= KEYS[4]

--parameters
local timestamp			= tonumber(ARGV[1])

local processing = redis.call('hgetall', processingHash)

for i=1, #processing, 2 do

	local taskInfo = cjson.decode(processing[i+1])

	if (taskInfo['metadata']['started'] + taskInfo['metadata']['timeout']) < timestamp then

		taskInfo['metadata']['attempts'] = taskInfo['metadata']['attempts'] - 1 
		local task = taskInfo['metadata']['task']

		if taskInfo['metadata']['attempts'] > 0 then
			--add to released list again
			taskInfo['metadata']['started'] = nil
			redis.call('rpush', releasedList, cjson.encode(taskInfo))

			redis.call('hset', closedHash, task, cjson.encode(taskInfo))
		else
			--add to failed list
			redis.call('hdel', closedHash, task)

			local taskInfo = {task=task, failed=timestamp}
			redis.call('rpush', failedList, cjson.encode(taskInfo))
		end

		--delete from processing
		redis.call('hdel', processingHash, task)
	end
end
