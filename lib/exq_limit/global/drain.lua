local version_key, allocation_key, heartbeat_key = KEYS[1], KEYS[2], KEYS[3]
local node_id, node_version, time, cutoff_time, amount = ARGV[1], tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4]), tonumber(ARGV[5])

local global_version = tonumber(redis.call('GET', version_key))
local allocation = nil

if global_version ~= node_version then
   return 0
else
   redis.call('ZREM', heartbeat_key, node_id)
   redis.call('ZADD', heartbeat_key, time, node_id)

   local exists = redis.call('HEXISTS', allocation_key, node_id)
   local stale_node_ids = redis.call('ZRANGEBYSCORE', heartbeat_key, 0, cutoff_time)
   if #stale_node_ids > 0 or exists ~= 1 then
      return 0
   end

   local node_ids = redis.call('ZRANGEBYSCORE', heartbeat_key, 0, '+inf')
   local i = amount
   while i > 0 and #node_ids > 0 do
      local nd = table.remove(node_ids, 1)
      allocation = cjson.decode(redis.call('HGET', allocation_key, nd))
      if allocation["current"] < allocation["allowed"] then
         local diff = allocation["allowed"] - allocation["current"]
         allocation["current"] = allocation["current"] + math.min(diff, i)
         redis.call('HSET', allocation_key, nd, cjson.encode(allocation))
         i = i - diff
      end
   end


   allocation = cjson.decode(redis.call('HGET', allocation_key, node_id))
   allocation["current"] = allocation["current"] - amount
   redis.call('HSET', allocation_key, node_id, cjson.encode(allocation))
   return 1
end
