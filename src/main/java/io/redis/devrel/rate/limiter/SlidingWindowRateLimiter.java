package io.redis.devrel.rate.limiter;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.params.SetParams;

import java.time.Duration;
import java.util.List;

public class SlidingWindowRateLimiter {

    private final JedisPool jedisPool;
    private final String key;
    private final int maxRequests;
    private final long windowSizeInMillis;

    public SlidingWindowRateLimiter(JedisPool jedisPool, String key, int maxRequests, Duration windowSize) {
        this.jedisPool = jedisPool;
        this.key = key;
        this.maxRequests = maxRequests;
        this.windowSizeInMillis = windowSize.toMillis();
    }

    public boolean allowRequest() {
        long now = System.currentTimeMillis();
        long windowStart = now - windowSizeInMillis;

        try (Jedis jedis = jedisPool.getResource()) {
            Transaction transaction = jedis.multi();
            transaction.zremrangeByScore(key, 0, windowStart);
            transaction.zcard(key);
            transaction.zadd(key, now, String.valueOf(now));
            transaction.pexpire(key, windowSizeInMillis);

            List<Object> results = transaction.exec();

            if (results == null || results.size() < 2) {
                return false;
            }

            Long requestsInWindow = (Long) results.get(1);
            return requestsInWindow < maxRequests;
        }
    }

}