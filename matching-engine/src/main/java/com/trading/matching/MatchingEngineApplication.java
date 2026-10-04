package com.trading.matching;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * MATCHING ENGINE (STUB) - minh hoạ IDEMPOTENT CONSUMER,
 * nửa còn lại của Transactional Outbox.
 *
 * Outbox đảm bảo AT-LEAST-ONCE (event có thể tới >= 1 lần).
 * Idempotent consumer biến at-least-once thành EFFECTIVELY-ONCE:
 * mỗi clientOrderId chỉ được xử lý đúng 1 lần dù message tới bao nhiêu lần.
 *
 * Cặp đôi Outbox + Idempotent Consumer chính là cách thế giới microservices
 * đạt "distributed transaction" mà không cần XA/2PC.
 */
// scanBasePackages: OutboxRelay nam o package com.trading.outbox (module outbox-support),
// NGOAI com.trading.matching nen component scan mac dinh khong thay -> relay khong chay,
// execution report khong bao gio ve lai order-service.
@SpringBootApplication(scanBasePackages = {"com.trading.matching", "com.trading.outbox"})
@EnableScheduling
public class MatchingEngineApplication {
    public static void main(String[] args) {
        SpringApplication.run(MatchingEngineApplication.class, args);
    }
}
