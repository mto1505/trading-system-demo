package com.trading.order.model;

import jakarta.validation.constraints.*;

import java.math.BigDecimal;

public record OrderRequest(
        // clientOrderId do CLIENT sinh (UUID) - chìa khoá idempotency:
        // client timeout rồi retry cùng clientOrderId -> nhận lại KẾT QUẢ CŨ,
        // không tạo lệnh trùng. Bắt buộc với API tài chính.
        @NotBlank String clientOrderId,
        @NotBlank String symbol,
        @NotNull @Pattern(regexp = "BUY|SELL") String side,
        @NotNull @Positive BigDecimal price,
        @Min(1) long quantity,
        @NotBlank String accountId
) {}
