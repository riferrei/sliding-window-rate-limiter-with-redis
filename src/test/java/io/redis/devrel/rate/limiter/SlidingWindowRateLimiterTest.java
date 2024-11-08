package io.redis.devrel.rate.limiter;

import com.redis.testcontainers.RedisStackContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.time.Duration;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
public class SlidingWindowRateLimiterTest {

    private static final Logger logger = Logger.getLogger(SlidingWindowRateLimiterTest.class.getName());

    @Container
    private static RedisStackContainer redisContainer =
            new RedisStackContainer(
                    RedisStackContainer.DEFAULT_IMAGE_NAME
                            .withTag("7.4.0-v1"));

    private static JedisPool jedisPool;
    private static SlidingWindowRateLimiter rateLimiter;

    @BeforeAll
    public static void setUp() {
        String address = redisContainer.getHost();
        Integer port = redisContainer.getFirstMappedPort();
        jedisPool = new JedisPool(address, port);
    }

    @AfterAll
    public static void tearDown() {
        jedisPool.close();
    }

    @Test
    void testBasicRateLimiting() {
        SlidingWindowRateLimiter rateLimiter = new SlidingWindowRateLimiter(jedisPool, "test:basic", 5, Duration.ofSeconds(10));

        for (int i = 0; i < 5; i++) {
            assertTrue(rateLimiter.allowRequest(), "Request " + (i + 1) + " should be allowed");
        }
        assertFalse(rateLimiter.allowRequest(), "Request 6 should be denied");
    }

    @Test
    void testVeryShortWindow() throws InterruptedException {
        rateLimiter = new SlidingWindowRateLimiter(jedisPool, "test:short", 1, Duration.ofMillis(100));

        assertTrue(rateLimiter.allowRequest(), "Request 1 should be allowed");
        assertFalse(rateLimiter.allowRequest(), "Request 2 should be denied");

        Thread.sleep(100); // Wait for 100 milliseconds

        assertTrue(rateLimiter.allowRequest(), "Request 3 should be allowed after 100ms");
    }

    @Test
    void testLongWindow() throws InterruptedException {
        rateLimiter = new SlidingWindowRateLimiter(jedisPool, "test:long", 2, Duration.ofSeconds(10));

        assertTrue(rateLimiter.allowRequest(), "Request 1 should be allowed");
        assertTrue(rateLimiter.allowRequest(), "Request 2 should be allowed");
        assertFalse(rateLimiter.allowRequest(), "Request 3 should be denied");

        Thread.sleep(5000); // Wait for 5 seconds

        assertFalse(rateLimiter.allowRequest(), "Request 4 should still be denied after 5 seconds");

        Thread.sleep(5000); // Wait for another 5 seconds (10 seconds total)

        assertTrue(rateLimiter.allowRequest(), "Request 5 should be allowed after 10 seconds");
    }

    @Test
    void testMultipleKeys() {
        SlidingWindowRateLimiter limiter1 = new SlidingWindowRateLimiter(jedisPool, "test:multi1", 2, Duration.ofSeconds(5));
        SlidingWindowRateLimiter limiter2 = new SlidingWindowRateLimiter(jedisPool, "test:multi2", 2, Duration.ofSeconds(5));

        assertTrue(limiter1.allowRequest(), "Request 1 for limiter1 should be allowed");
        assertTrue(limiter1.allowRequest(), "Request 2 for limiter1 should be allowed");
        assertFalse(limiter1.allowRequest(), "Request 3 for limiter1 should be denied");

        assertTrue(limiter2.allowRequest(), "Request 1 for limiter2 should be allowed");
        assertTrue(limiter2.allowRequest(), "Request 2 for limiter2 should be allowed");
        assertFalse(limiter2.allowRequest(), "Request 3 for limiter2 should be denied");
    }

    @Test
    void testSlidingWindowBehavior() throws InterruptedException {
        rateLimiter = new SlidingWindowRateLimiter(jedisPool, "test:sliding", 3, Duration.ofSeconds(5));

        assertTrue(rateLimiter.allowRequest(), "Request 1 should be allowed");
        assertTrue(rateLimiter.allowRequest(), "Request 2 should be allowed");
        assertTrue(rateLimiter.allowRequest(), "Request 3 should be allowed");
        assertFalse(rateLimiter.allowRequest(), "Request 4 should be denied");

        Thread.sleep(2000); // Wait for 2 seconds

        assertTrue(rateLimiter.allowRequest(), "Request 5 should be allowed after 2 seconds");
        assertTrue(rateLimiter.allowRequest(), "Request 6 should be allowed");
        assertFalse(rateLimiter.allowRequest(), "Request 7 should be denied");

        Thread.sleep(3000); // Wait for another 3 seconds (5 seconds total from start)

        assertTrue(rateLimiter.allowRequest(), "Request 8 should be allowed after 5 seconds");
        assertTrue(rateLimiter.allowRequest(), "Request 9 should be allowed");
        assertTrue(rateLimiter.allowRequest(), "Request 10 should be allowed");
        assertFalse(rateLimiter.allowRequest(), "Request 11 should be denied");
    }

}