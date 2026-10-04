package com.trading.common.model;

import java.math.BigDecimal;

/**
 * CONTRACT DÙNG CHUNG giữa các service (single source of truth).
 * Trước đây mỗi service tự định nghĩa record giống hệt nhau -> "contract ngầm",
 * đổi 1 field ở gateway mà quên sửa ở pricing là lỗi runtime khó tìm.
 * Giờ đổi schema = đổi 1 chỗ, compiler bắt lỗi ở mọi service.
 *
 * Bước tiến hoá tiếp theo (khi nhiều team/nhiều ngôn ngữ): Avro/Protobuf
 * + Schema Registry để có schema evolution kiểm soát bởi registry thay vì compiler.
 */
public record TickEvent(
        String symbol,       // Mã CK: VN30F2607, HPG, ...
        BigDecimal price,    // BigDecimal, KHÔNG dùng double cho tiền tệ
        long volume,
        long eventTime       // epoch millis tại nguồn -> đo latency E2E
) {}
