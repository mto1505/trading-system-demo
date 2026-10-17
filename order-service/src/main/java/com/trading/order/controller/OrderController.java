package com.trading.order.controller;

import com.trading.common.model.OrderResult;
import com.trading.order.model.OrderRequest;
import com.trading.order.service.OrderService;
import io.github.resilience4j.ratelimiter.RequestNotPermitted;
import io.github.resilience4j.ratelimiter.annotation.RateLimiter;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@CrossOrigin("*")
@RestController
@RequestMapping("/api/v1/orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    /**
     * RateLimiter 200 req/s (cấu hình trong yml): chặn máy đặt lệnh loạn /
     * client bug retry vô hạn trước khi nó đè chết DB và Kafka.
     * Vượt ngưỡng -> RequestNotPermitted -> HTTP 429 (handler bên dưới).
     */
    @PostMapping
    @RateLimiter(name = "orders")
    public ResponseEntity<OrderResult> place(@Valid @RequestBody OrderRequest request) {
        OrderResult result = orderService.placeOrder(request);
        HttpStatus status = "ACCEPTED".equals(result.status())
                ? HttpStatus.CREATED : HttpStatus.UNPROCESSABLE_ENTITY;
        return ResponseEntity.status(status).body(result);
    }

    @ExceptionHandler(RequestNotPermitted.class)
    public ResponseEntity<Map<String, String>> rateLimited() {
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                .body(Map.of("error", "rate limit exceeded, retry later"));
    }
}
