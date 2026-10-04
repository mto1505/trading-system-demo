package com.trading.matching.service;

import com.trading.common.model.OrderResult;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * Luật giao dịch tại sàn - KHÁC với pre-trade check ở order-service:
 *
 *   order-service check   = bảo vệ KHÁCH HÀNG (fat-finger, data cũ, rủi ro cao)
 *   matching-engine check = luật của SÀN (biên độ, lô, phiên) - là nguồn sự thật
 *
 * Hai tầng cố tình trùng lặp một phần: order-service từ chối sớm cho nhanh,
 * nhưng KHÔNG được tin tưởng - client có thể gọi thẳng, hoặc tham số hai bên
 * lệch nhau. Sàn luôn kiểm tra lại. Đây là lý do saga cần chiều về:
 * lệnh qua được cửa 1 vẫn có thể chết ở cửa 2.
 */
@Component
public class TradingRules {

    @Value("${matching.price-band-pct:7}")
    private BigDecimal priceBandPct;      // HOSE 7%, HNX 10%, phái sinh theo VN30

    @Value("${matching.lot-size:100}")
    private long lotSize;                 // cổ phiếu lô chẵn 100; phái sinh = 1

    @Value("${matching.session-check-enabled:false}")
    private boolean sessionCheckEnabled;  // tắt mặc định để demo chạy 24/7

    @Value("${matching.session-zone:Asia/Ho_Chi_Minh}")
    private String sessionZone;

    /** @return null nếu hợp lệ, ngược lại trả lý do từ chối. */
    public String validate(OrderResult order) {
        if (order.quantity() <= 0 || order.quantity() % lotSize != 0) {
            return "INVALID_LOT_SIZE must be multiple of " + lotSize;
        }

        BigDecimal ref = order.refPrice() != null ? order.refPrice() : order.price();
        if (ref == null || ref.signum() <= 0) {
            return "NO_REFERENCE_PRICE";
        }
        BigDecimal deviation = order.price().subtract(ref).abs()
                .divide(ref, 6, RoundingMode.HALF_UP)
                .multiply(BigDecimal.valueOf(100));
        if (deviation.compareTo(priceBandPct) > 0) {
            return "PRICE_OUT_OF_BAND " + deviation.setScale(2, RoundingMode.HALF_UP)
                    + "% > " + priceBandPct + "%";
        }

        if (sessionCheckEnabled && !inSession()) {
            return "MARKET_CLOSED";
        }
        return null;
    }

    private boolean inSession() {
        LocalTime now = ZonedDateTime.now(ZoneId.of(sessionZone)).toLocalTime();
        boolean morning = !now.isBefore(LocalTime.of(9, 0)) && !now.isAfter(LocalTime.of(11, 30));
        boolean afternoon = !now.isBefore(LocalTime.of(13, 0)) && !now.isAfter(LocalTime.of(14, 45));
        return morning || afternoon;
    }
}
