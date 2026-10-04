package com.trading.order.consumer;

import com.trading.common.model.ExecutionReport;
import com.trading.order.service.OrderStatusService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ExecutionReportConsumer {

    private static final int MAX_ATTEMPTS = 3;

    private final OrderStatusService orderStatusService;

    public ExecutionReportConsumer(OrderStatusService orderStatusService) {
        this.orderStatusService = orderStatusService;
    }

    @KafkaListener(topics = "stock.executions",
                   groupId = "order-service-exec-group",
                   containerFactory = "executionListenerFactory")
    public void onExecutionReport(ExecutionReport report) {
        // Retry vòng ngoài (không trong @Transactional) vì transaction đã rollback
        // rồi mới ném exception - phải mở transaction MỚI để thử lại.
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            try {
                orderStatusService.apply(report);
                return;
            } catch (OptimisticLockingFailureException ex) {
                log.warn("Xung dot optimistic lock lan {} cho clientOrderId={}",
                        attempt, report.clientOrderId());
                if (attempt == MAX_ATTEMPTS) {
                    // Ném ra -> error handler retry rồi đẩy DLT (đã cấu hình ở container factory)
                    throw ex;
                }
            }
        }
    }
}
