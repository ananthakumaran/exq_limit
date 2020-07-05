local version_key, allocation_key, heartbeat_key = KEYS[1], KEYS[2], KEYS[3]
local node_id, node_version, time, cutoff_time = ARGV[1], tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4])

local global_version = tonumber(redis.call('GET', version_key))

if global_version ~= node_version then
   return 0
else
   redis.call('ZREM', heartbeat_key, node_id)
   redis.call('ZADD', heartbeat_key, time, node_id)

   local exists = redis.call('HEXISTS', allocation_key, node_id)
   local stale_node_ids = redis.call('ZRANGEBYSCORE', heartbeat_key, 0, cutoff_time)
   if #stale_node_ids > 0 or exists ~= 1 then
      return 0
   else
      local allocation = cjson.decode(redis.call('HGET', allocation_key, node_id))
      return {allocation["allowed"], allocation["current"]}
   end
end
