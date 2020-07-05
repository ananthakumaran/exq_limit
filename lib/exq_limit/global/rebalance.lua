local version_key, limit_key, allocation_key, heartbeat_key = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
local node_id, limit, time, cutoff_time = ARGV[1], tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4])
local global_version = 0
local allocation = nil

redis.call('ZREM', heartbeat_key, node_id)
redis.call('ZADD', heartbeat_key, time, node_id)


local global_limit = tonumber(redis.call('GET', limit_key))
local exists = redis.call('HEXISTS', allocation_key, node_id)
local stale_node_ids = redis.call('ZRANGEBYSCORE', heartbeat_key, 0, cutoff_time)
-- if this is a node restart, there is no need to do reallocation.
if global_limit ~= limit or exists ~= 1 or #stale_node_ids ~= 0 then
   for i = 1, #stale_node_ids do
      redis.call('HDEL', allocation_key, stale_node_ids[i])
   end
   redis.call('ZREMRANGEBYSCORE', heartbeat_key, 0, cutoff_time)

   local node_ids = redis.call('ZRANGEBYSCORE', heartbeat_key, 0, '+inf')

   local node_allowed = math.floor(limit / #node_ids)
   local remainder = limit % #node_ids
   local total_current = 0
   for i, nd in ipairs(node_ids) do
      local allowed = node_allowed
      if i <= remainder then
         allowed = allowed + 1
      end
      allocation = cjson.decode(redis.call('HGET', allocation_key, nd) or '{"current": 0, "allowed": 0}')
      total_current = total_current + allocation["current"]
      redis.call('HSET', allocation_key, nd, cjson.encode({current = allocation["current"], allowed = allowed}))
   end

   if total_current < limit then
      allocation = cjson.decode(redis.call('HGET', allocation_key, node_id))
      allocation["current"] = allocation["current"] + limit - total_current
      redis.call('HSET', allocation_key, node_id, cjson.encode(allocation))
   end

   redis.call('SET', limit_key, limit)
   global_version = tonumber(redis.call('INCR', version_key))
else
   global_version = tonumber(redis.call('GET', version_key))
end

allocation = cjson.decode(redis.call('HGET', allocation_key, node_id))
return {global_version, allocation["allowed"], allocation["current"]}
