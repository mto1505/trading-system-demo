package com.trading.pricing.service;

import com.trading.common.model.EnrichedMarketData;
import com.trading.common.model.TickEvent;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Bộ tính toán rủi ro streaming - O(1) mỗi tick.
 *
 * Kỹ thuật: dùng Welford-style incremental stats trên cửa sổ trượt log-return,
 * KHÔNG lưu toàn bộ lịch sử -> memory cố định, tốc độ ổn định.
 *
 * State per-symbol được bảo vệ bằng synchronized trên chính object state.
 * Vì Kafka key = symbol nên mỗi symbol chỉ được 1 consumer thread xử lý
 * -> contention gần như bằng 0 (lock luôn uncontended).
 */
@Component
public class RiskCalculator {

    private static final int WINDOW = 120;           // 120 tick gần nhất
    private static final double ANNUALIZE = Math.sqrt(252 * 6.5 * 3600); // giả định tick ~1s
    private static final double Z_95 = 1.645;        // z-score cho VaR 95%
    private static final double EMA_ALPHA = 0.1;

    private final Map<String, SymbolState> states = new ConcurrentHashMap<>();

    public EnrichedMarketData onTick(TickEvent tick) {
        SymbolState st = states.computeIfAbsent(tick.symbol(), s -> new SymbolState());
        synchronized (st) {
            return st.update(tick);
        }
    }

    private static class SymbolState {
        double lastPrice = Double.NaN;
        double ema = Double.NaN;
        final Deque<Double> returns = new ArrayDeque<>(WINDOW);
        double sum = 0, sumSq = 0; // incremental mean/variance

        EnrichedMarketData update(TickEvent tick) {
            double px = tick.price().doubleValue();

            // 1) EMA giá
            ema = Double.isNaN(ema) ? px : EMA_ALPHA * px + (1 - EMA_ALPHA) * ema;

            // 2) Log-return + sliding window variance (O(1))
            if (!Double.isNaN(lastPrice) && lastPrice > 0) {
                double r = Math.log(px / lastPrice);
                if (returns.size() == WINDOW) {
                    double old = returns.pollFirst();
                    sum -= old; sumSq -= old * old;
                }
                returns.addLast(r);
                sum += r; sumSq += r * r;
            }
            lastPrice = px;

            int n = returns.size();
            double vol = 0, var95 = 0;
            if (n >= 2) {
                double mean = sum / n;
                double variance = Math.max(0, (sumSq / n) - mean * mean);
                double stdPerTick = Math.sqrt(variance);
                vol = stdPerTick * ANNUALIZE * 100;      // % năm hoá
                var95 = Z_95 * stdPerTick * 100;         // % VaR 1-tick horizon
            }

            String riskLevel = vol < 15 ? "LOW" : vol < 35 ? "MEDIUM" : "HIGH";

            return new EnrichedMarketData(
                    tick.symbol(),
                    tick.price(),
                    bd(ema, 4),
                    bd(vol, 6),
                    bd(var95, 6),
                    riskLevel,
                    System.currentTimeMillis(),
                    tick.eventTime()
            );
        }

        private static BigDecimal bd(double v, int scale) {
            if (Double.isNaN(v) || Double.isInfinite(v)) return BigDecimal.ZERO;
            return new BigDecimal(v, MathContext.DECIMAL64).setScale(scale, RoundingMode.HALF_UP);
        }
    }
}
