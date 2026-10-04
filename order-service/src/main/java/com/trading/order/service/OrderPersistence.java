package com.trading.order.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.common.model.OrderResult;
import com.trading.order.entity.OrderEntity;
import com.trading.order.entity.OutboxEventEntity;
import com.trading.order.model.OrderRequest;
import com.trading.order.repository.OrderRepository;
import com.trading.order.repository.OutboxEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;

@Service
public class OrderPersistence {

    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxRepository;
    private final ObjectMapper objectMapper;

    public OrderPersistence(OrderRepository orderRepository,
                            OutboxEventRepository outboxRepository,
                            ObjectMapper objectMapper) {
        this.orderRepository = orderRepository;
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
    }

    /**
     * TRÁI TIM CỦA OUTBOX PATTERN: 2 INSERT, 1 transaction, 1 database.
     * Hoặc cả lệnh lẫn event cùng tồn tại, hoặc không cái nào - không có
     * trạng thái "lệnh có mà event mất" hay ngược lại.
     */
    @Transactional
    public void saveWithOutbox(OrderRequest req, OrderResult result) {
        OrderEntity order = new OrderEntity();
        order.setOrderId(result.orderId());
        order.setClientOrderId(result.clientOrderId());
        order.setStatus(result.status());
        order.setReason(result.reason());
        order.setSymbol(result.symbol());
        order.setSide(result.side());
        order.setPrice(result.price());
        order.setQuantity(result.quantity());
        order.setRefPrice(result.refPrice());
        order.setRiskLevel(result.riskLevel());
        order.setAccountId(req.accountId());
        order.setCreatedAt(Instant.now());
        orderRepository.save(order);

        // Chỉ lệnh ACCEPTED mới sinh event cho hệ khớp lệnh
        if ("ACCEPTED".equals(result.status())) {
            OutboxEventEntity event = new OutboxEventEntity();
            event.setAggregateType("ORDER");
            event.setAggregateId(result.orderId());
            event.setEventType("ORDER_ACCEPTED");
            event.setTopic("stock.orders");
            event.setMsgKey(result.symbol());   // key = symbol giữ thứ tự theo mã
            event.setPayload(toJson(result));
            event.setCreatedAt(Instant.now());
            outboxRepository.save(event);
        }
    }

    @Transactional(readOnly = true)
    public Optional<OrderResult> findExisting(String clientOrderId) {
        return orderRepository.findByClientOrderId(clientOrderId).map(o -> new OrderResult(
                o.getOrderId(), o.getClientOrderId(), o.getStatus(), o.getReason(),
                o.getSymbol(), o.getSide(), o.getPrice(), o.getQuantity(),
                o.getRefPrice(), o.getRiskLevel(), o.getCreatedAt().toEpochMilli()));
    }

    private String toJson(OrderResult result) {
        try {
            return objectMapper.writeValueAsString(result);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Serialize OrderResult that bai", e);
        }
    }
}
