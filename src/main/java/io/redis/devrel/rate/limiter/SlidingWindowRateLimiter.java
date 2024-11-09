package io.redis.devrel.rate.limiter;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;

import java.time.Duration;
import java.util.List;
import java.util.function.Supplier;
import java.util.logging.Logger;

public class SlidingWindowRateLimiter {

    private static final Logger logger = Logger.getLogger(SlidingWindowRateLimiter.class.getName());

    private final JedisPool jedisPool;
    private final String key;
    private final int maxRequests;
    private final long windowSizeInMillis;
    private final Supplier<Long> timeProvider;

    public SlidingWindowRateLimiter(JedisPool jedisPool, String key, int maxRequests, Duration windowSize) {
        this(jedisPool, key, maxRequests, windowSize, System::currentTimeMillis);
    }

    // Constructor for testing
    SlidingWindowRateLimiter(JedisPool jedisPool, String key, int maxRequests, Duration windowSize, Supplier<Long> timeProvider) {
        this.jedisPool = jedisPool;
        this.key = key;
        this.maxRequests = maxRequests;
        this.windowSizeInMillis = windowSize.toMillis();
        this.timeProvider = timeProvider;
    }

    public boolean allowRequest() {
        long now = timeProvider.get();
        long windowStart = now - windowSizeInMillis;

        try (Jedis jedis = jedisPool.getResource()) {
            Transaction transaction = jedis.multi();
            transaction.zremrangeByScore(key, 0, windowStart);
            transaction.zrange(key, 0, -1);
            transaction.zadd(key, now, String.valueOf(now));
            transaction.pexpire(key, windowSizeInMillis);

            List<Object> results = transaction.exec();

            if (results == null || results.size() < 2) {
                logger.warning("Redis transaction failed");
                return false;
            }

            List<String> requests = (List<String>) results.get(1);
            int requestCount = requests.size();
            boolean allowed = requestCount < maxRequests;

            logger.info(String.format("Time: %d, Requests in window: %d, Max requests: %d, Allowed: %b",
                    now, requestCount, maxRequests, allowed));

            return allowed;
        }
    }
}