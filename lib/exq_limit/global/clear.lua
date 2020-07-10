local version_key, allocation_key, heartbeat_key = KEYS[1], KEYS[2], KEYS[3]
local node_id = ARGV[1]

redis.call('ZREM', heartbeat_key, node_id)
redis.call('HDEL', allocation_key, node_id)
redis.call('INCR', version_key)

return 1
