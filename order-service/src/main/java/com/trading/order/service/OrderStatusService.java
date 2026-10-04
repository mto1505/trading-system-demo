package com.trading.order.service;

import com.trading.common.model.ExecutionReport;
import com.trading.order.entity.OrderEntity;
import com.trading.order.repository.OrderRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;

@Slf4j
@Service
public class OrderStatusService {

    private final OrderRepository repository;
    private final Counter filled;
    private final Counter rejectedByMatching;
    private final Counter ignored;

    public OrderStatusService(OrderRepository repository, MeterRegistry registry) {
        this.repository = repository;
        this.filled = Counter.builder("orders.filled").register(registry);
        this.rejectedByMatching = Counter.builder("orders.rejected.by.matching").register(registry);
        this.ignored = Counter.builder("orders.report.ignored").register(registry);
    }

    /**
     * Áp execution report lên lệnh. @Version lo chuyện hai report tới song song:
     * pod thua cuộc ném OptimisticLockingFailureException -> consumer retry.
     */
    @Transactional
    public void apply(ExecutionReport report) {
        Optional<OrderEntity> found = repository.findByClientOrderId(report.clientOrderId());
        if (found.isEmpty()) {
            // Về lý thuyết không xảy ra: lệnh được COMMIT trước khi outbox event ra Kafka.
            // Log để phát hiện nếu ai đó publish thẳng vào stock.orders (bỏ qua outbox).
            log.warn("Nhan execution report cho lenh khong ton tai clientOrderId={}",
                    report.clientOrderId());
            ignored.increment();
            return;
        }

        OrderEntity order = found.get();
        String target = "FILLED".equals(report.status())
                ? OrderStateMachine.FILLED
                : OrderStateMachine.REJECTED_BY_MATCHING;

        if (!OrderStateMachine.canTransition(order.getStatus(), target)) {
            // Report trùng (redelivery) hoặc đến muộn -> bỏ qua, KHÔNG phải lỗi
            log.debug("Bo qua report: {} -> {} khong hop le (clientOrderId={})",
                    order.getStatus(), target, report.clientOrderId());
            ignored.increment();
            return;
        }

        order.setStatus(target);
        order.setReason(report.reason());
        order.setFilledQty(report.filledQty());
        order.setAvgFillPrice(report.fillPrice());
        order.setUpdatedAt(Instant.now());

        if (OrderStateMachine.FILLED.equals(target)) {
            filled.increment();
            log.info("Lenh {} da KHOP {} @ {}", report.clientOrderId(),
                    report.filledQty(), report.fillPrice());
        } else {
            rejectedByMatching.increment();
            log.info("Lenh {} bi matching TU CHOI: {}", report.clientOrderId(), report.reason());
        }
    }
}
