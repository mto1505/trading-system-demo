package com.trading.order.controller;

import com.trading.common.model.OrderResult;
import com.trading.order.entity.OrderEntity;
import com.trading.order.model.OrderRequest;
import com.trading.order.model.OrderView;
import com.trading.order.repository.OrderRepository;
import com.trading.order.service.OrderService;
import io.github.resilience4j.ratelimiter.RequestNotPermitted;
import io.github.resilience4j.ratelimiter.annotation.RateLimiter;
import jakarta.validation.Valid;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@CrossOrigin("*")
@RestController
@RequestMapping("/api/v1/orders")
public class OrderController {

    /** Tran cung cho size: khong de client tu dat 1_000_000 roi keo sap pod. */
    private static final int MAX_PAGE_SIZE = 200;

    private final OrderService orderService;
    /* Doc thang tu repository: cac endpoint GET chi la truy van thuan, khong co
     * nghiep vu nao de dat trong OrderService. */
    private final OrderRepository orderRepository;

    public OrderController(OrderService orderService, OrderRepository orderRepository) {
        this.orderService = orderService;
        this.orderRepository = orderRepository;
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


    /**
     * Liệt kê lệnh, mới nhất trước.
     *
     * LUÔN phân trang: bảng orders giữ cả lệnh REJECTED để audit nên chỉ có
     * tăng. `size` bị chặn cứng ở 200 - không để client tự đặt 1_000_000 rồi
     * kéo sập pod.
     *
     * Lọc được theo status và accountId; để trống là lấy tất cả.
     */
    @GetMapping
    @RateLimiter(name = "orders")
    public Page<OrderView> list(
            @RequestParam(required = false) String status,
            @RequestParam(required = false) String accountId,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {

        int safeSize = Math.min(Math.max(size, 1), MAX_PAGE_SIZE);
        Pageable p = PageRequest.of(Math.max(page, 0), safeSize,
                Sort.by(Sort.Direction.DESC, "createdAt"));

        String st = (status == null || status.isBlank()) ? null : status.trim().toUpperCase();
        String acc = (accountId == null || accountId.isBlank()) ? null : accountId.trim();

        Page<OrderEntity> found;
        if (acc != null && st != null)      found = orderRepository.findAllByAccountIdAndStatus(acc, st, p);
        else if (acc != null)               found = orderRepository.findAllByAccountId(acc, p);
        else if (st != null)                found = orderRepository.findAllByStatus(st, p);
        else                                found = orderRepository.findAll(p);

        return found.map(OrderView::from);
    }

    /**
     * Tra một lệnh theo clientOrderId - khoá mà CLIENT tự sinh, nên tra được
     * ngay cả khi client không giữ orderId do server cấp.
     *
     * Dùng clientOrderId chứ không dùng orderId vì đây chính là chìa khoá
     * idempotency: client timeout rồi muốn biết lệnh cũ ra sao thì nó chỉ có
     * trong tay clientOrderId.
     */
    @GetMapping("/{clientOrderId}")
    @RateLimiter(name = "orders")
    public ResponseEntity<OrderView> getByClientOrderId(@PathVariable String clientOrderId) {
        return orderRepository.findByClientOrderId(clientOrderId)
                .map(OrderView::from)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
    @ExceptionHandler(RequestNotPermitted.class)
    public ResponseEntity<Map<String, String>> rateLimited() {
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                .body(Map.of("error", "rate limit exceeded, retry later"));
    }
}
