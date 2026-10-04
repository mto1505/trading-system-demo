package com.trading.gateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * MARKET GATEWAY - Cổng nhận dữ liệu thị trường từ bên ngoài.
 *
 * Vai trò: Nhận tick data (giá, khối lượng) từ nguồn ngoài
 * (exchange feed, REST API, hoặc simulated feed) rồi đẩy vào Kafka.
 *
 * Nguyên tắc thiết kế: Gateway KHÔNG xử lý logic nghiệp vụ.
 * Nó chỉ validate tối thiểu + serialize + produce vào Kafka càng nhanh càng tốt.
 * Việc tính toán nặng để service phía sau lo (tách biệt trách nhiệm).
 */
@SpringBootApplication
@EnableScheduling
public class MarketGatewayApplication {
    public static void main(String[] args) {
        SpringApplication.run(MarketGatewayApplication.class, args);
    }
}
