-- Atomic reservation. Redis runs this whole script before any other command,
-- so two requests can never both take the last phone.
--
-- KEYS[1] stock   KEYS[2] buyers   KEYS[3] status   KEYS[4] outbox
-- KEYS[5] feed    KEYS[6] metrics key for the current second   KEYS[7] sale start (ms)
-- ARGV[1] userId  ARGV[2] new orderId  ARGV[3] productId  ARGV[4] now (ms)
--
-- Returns {code, orderId, stockLeft}
--   1 = reserved now   2 = this user already has an order   0 = sold out
--   3 = sale has not started (orderId field carries the start time)

local start = tonumber(redis.call('GET', KEYS[7]) or '0')
if tonumber(ARGV[4]) < start then
  return {3, tostring(start), 0}
end

local existing = redis.call('HGET', KEYS[2], ARGV[1])
if existing then
  redis.call('HINCRBY', KEYS[6], 'duplicate', 1)
  redis.call('EXPIRE', KEYS[6], 300)
  return {2, existing, tonumber(redis.call('GET', KEYS[1]) or '0')}
end

local stock = tonumber(redis.call('GET', KEYS[1]) or '0')
if stock <= 0 then
  redis.call('HINCRBY', KEYS[6], 'sold_out', 1)
  redis.call('EXPIRE', KEYS[6], 300)
  return {0, '', 0}
end

local left = redis.call('DECR', KEYS[1])                -- take one phone
redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])           -- remember the buyer
redis.call('HSET', KEYS[3], ARGV[2], 'QUEUED')
-- Transactional outbox: the order message is written in the SAME atomic step
-- as the stock change, so we can never take stock without queueing an order.
redis.call('XADD', KEYS[4], '*',
  'order_id', ARGV[2], 'user_id', ARGV[1], 'product_id', ARGV[3], 'reserved_at', ARGV[4])
redis.call('LPUSH', KEYS[5], ARGV[2] .. '|' .. ARGV[4])
redis.call('LTRIM', KEYS[5], 0, 19)
redis.call('HINCRBY', KEYS[6], 'accepted', 1)
redis.call('EXPIRE', KEYS[6], 300)
return {1, ARGV[2], left}
