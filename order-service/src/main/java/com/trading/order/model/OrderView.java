package com.trading.order.model;

import com.trading.order.entity.OrderEntity;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Bản chiếu CHỈ-ĐỌC của một lệnh, dùng cho các endpoint GET.
 *
 * Vì sao không trả thẳng OrderEntity:
 *
 *  1. Entity còn gắn với persistence context. Trả thẳng nó thì Jackson serialize
 *     lúc đã ra ngoài transaction, và mọi quan hệ LAZY sẽ nổ
 *     LazyInitializationException - lỗi chỉ xuất hiện khi có dữ liệu thật, không
 *     bao giờ thấy lúc unit test.
 *
 *  2. Thêm một cột vào bảng là tự động lọt ra API, kể cả cột nội bộ. Ở đây đang
 *     có `version` (optimistic locking) và `id` (khoá tự tăng) - hai thứ client
 *     không cần biết và không nên phụ thuộc vào.
 *
 *  3. Đổi tên cột trong DB sẽ âm thầm phá hợp đồng API. Có DTO thì compiler bắt
 *     được ngay tại chỗ map.
 */
public record OrderView(
        String orderId,
        String clientOrderId,
        String status,
        String reason,
        String symbol,
        String side,
        BigDecimal price,
        Long quantity,
        BigDecimal refPrice,
        String riskLevel,
        String accountId,
        Long filledQty,
        BigDecimal avgFillPrice,
        Instant createdAt,
        Instant updatedAt
) {
    public static OrderView from(OrderEntity e) {
        return new OrderView(
                e.getOrderId(),
                e.getClientOrderId(),
                e.getStatus(),
                e.getReason(),
                e.getSymbol(),
                e.getSide(),
                e.getPrice(),
                e.getQuantity(),
                e.getRefPrice(),
                e.getRiskLevel(),
                e.getAccountId(),
                e.getFilledQty(),
                e.getAvgFillPrice(),
                e.getCreatedAt(),
                e.getUpdatedAt()
        );
    }
}
