package com.trading.order.service;

import com.trading.order.entity.OutboxEventEntity;
import com.trading.order.repository.OutboxEventRepository;
import com.trading.outbox.OutboxStore;
import org.springframework.stereotype.Component;

import java.util.List;

/** ADAPTER nối bảng outbox của ordersvc vào relay dùng chung. */
@Component
public class JpaOutboxStore implements OutboxStore {

    private final OutboxEventRepository repository;

    public JpaOutboxStore(OutboxEventRepository repository) {
        this.repository = repository;
    }

    @Override
    public List<OutboxEventEntity> lockUnpublishedBatch(int limit) {
        return repository.lockUnpublishedBatch(limit);
    }
}
