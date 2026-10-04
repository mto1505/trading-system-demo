package com.trading.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * ORDER SERVICE - Service đặt lệnh.
 *
 * Điểm thiết kế quan trọng: order-service KHÔNG gọi REST sang pricing-risk-service
 * mỗi lần đặt lệnh (chậm + coupling). Thay vào đó nó SUBSCRIBE topic "stock.enriched"
 * và giữ LOCAL CACHE giá/rủi ro mới nhất.
 *
 * -> Pre-trade risk check chỉ là 1 lần đọc ConcurrentHashMap: micro-giây.
 * -> pricing-risk-service chết, order-service vẫn hoạt động (dữ liệu hơi cũ - trade-off).
 * Đây là pattern "local materialized view" / CQRS-read-model qua event.
 */
// scanBasePackages: OutboxRelay nam o package com.trading.outbox (module outbox-support),
// NGOAI com.trading.order nen component scan mac dinh khong thay -> relay khong chay,
// outbox_event khong bao gio duoc publish. Phai khai bao them package nay.
@SpringBootApplication(scanBasePackages = {"com.trading.order", "com.trading.outbox"})
@EnableScheduling
public class OrderServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(OrderServiceApplication.class, args);
    }
}
