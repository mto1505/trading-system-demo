package com.trading.matching.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.common.model.ExecutionReport;
import com.trading.common.model.OrderResult;
import com.trading.matching.entity.ExecutionEntity;
import com.trading.matching.entity.OutboxEventEntity;
import com.trading.matching.repository.ExecutionRepository;
import com.trading.matching.repository.OutboxEventRepository;
import com.trading.matching.repository.ProcessedEventRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;

@Slf4j
@Service
public class MatchingService {

    private final ProcessedEventRepository processedRepo;
    private final ExecutionRepository executionRepo;
    private final OutboxEventRepository outboxRepo;
    private final TradingRules rules;
    private final ObjectMapper objectMapper;
    private final Counter matched;
    private final Counter businessRejected;
    private final Counter duplicated;

    public MatchingService(ProcessedEventRepository processedRepo,
                           ExecutionRepository executionRepo,
                           OutboxEventRepository outboxRepo,
                           TradingRules rules,
                           ObjectMapper objectMapper,
                           MeterRegistry registry) {
        this.processedRepo = processedRepo;
        this.executionRepo = executionRepo;
        this.outboxRepo = outboxRepo;
        this.rules = rules;
        this.objectMapper = objectMapper;
        this.matched = Counter.builder("matching.executed").register(registry);
        this.businessRejected = Counter.builder("matching.rejected").register(registry);
        this.duplicated = Counter.builder("matching.duplicate.skipped").register(registry);
    }

    /**
     * MỘT TRANSACTION duy nhất gồm 3 việc:
     *   (1) ghi sổ processed_event  -> idempotency
     *   (2) ghi execution (nếu hợp lệ)
     *   (3) ghi outbox execution report -> saga chiều về
     *
     * Cả ba cùng commit hoặc cùng rollback. Đây là điểm mấu chốt: nếu (3)
     * nằm ngoài transaction, sẽ có trường hợp lệnh đã khớp mà order-service
     * không bao giờ biết -> lệnh treo ACCEPTED vĩnh viễn, tiền của khách bị giữ.
     */
    @Transactional
    public void process(OrderResult order) {
        if (processedRepo.insertIfAbsent(order.clientOrderId()) == 0) {
            duplicated.increment();
            log.info("Bo qua message trung clientOrderId={}", order.clientOrderId());
            return;
        }

        String violation = rules.validate(order);
        if (violation != null) {
            businessRejected.increment();
            log.info("Matching TU CHOI {}: {}", order.clientOrderId(), violation);
            emit(new ExecutionReport(order.orderId(), order.clientOrderId(), order.symbol(),
                    order.side(), "REJECTED", violation, null, 0L, System.currentTimeMillis()));
            return;
        }

        BigDecimal fillPrice = order.refPrice() != null ? order.refPrice() : order.price();
        ExecutionEntity exec = new ExecutionEntity();
        exec.setOrderId(order.orderId());
        exec.setClientOrderId(order.clientOrderId());
        exec.setSymbol(order.symbol());
        exec.setSide(order.side());
        exec.setFillPrice(fillPrice);
        exec.setQuantity(order.quantity());
        exec.setExecTime(Instant.now());
        executionRepo.save(exec);

        matched.increment();
        log.info("KHOP LENH {} {} {} x{} @ {}", order.clientOrderId(),
                order.side(), order.symbol(), order.quantity(), fillPrice);

        emit(new ExecutionReport(order.orderId(), order.clientOrderId(), order.symbol(),
                order.side(), "FILLED", null, fillPrice, order.quantity(),
                System.currentTimeMillis()));
    }

    private void emit(ExecutionReport report) {
        OutboxEventEntity event = new OutboxEventEntity();
        event.setAggregateId(report.clientOrderId());
        event.setEventType("EXECUTION_REPORT");
        event.setTopic("stock.executions");
        event.setMsgKey(report.symbol());   // cùng key với stock.orders -> giữ thứ tự theo mã
        event.setPayload(toJson(report));
        event.setCreatedAt(Instant.now());
        outboxRepo.save(event);
    }

    private String toJson(ExecutionReport report) {
        try {
            return objectMapper.writeValueAsString(report);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Serialize ExecutionReport that bai", e);
        }
    }
}
