package com.trading.pricing;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * PRICING-RISK SERVICE - Trái tim của hệ thống.
 *
 * Luồng xử lý:
 *   1. Consume batch tick từ Kafka topic "stock.ticks"
 *   2. Tính toán: giá EMA, volatility (độ biến động), VaR đơn giản, risk level
 *   3. Cập nhật snapshot IN-MEMORY (đọc nhanh <1ms) - hot path
 *   4. Publish kết quả enriched vào topic "stock.enriched" cho order-service
 *   5. Ghi PostgreSQL theo lô định kỳ (write-behind) - cold path
 *
 * Nguyên tắc tốc độ: TÁCH HOT PATH (tính toán, in-memory) khỏi COLD PATH (ghi DB).
 * Không bao giờ để I/O database chặn luồng xử lý tick.
 */
@SpringBootApplication
@EnableScheduling
public class PricingRiskApplication {
    public static void main(String[] args) {
        SpringApplication.run(PricingRiskApplication.class, args);
    }
}
