package com.trading.matching.service;

import com.trading.matching.entity.OutboxEventEntity;
import com.trading.matching.repository.OutboxEventRepository;
import com.trading.outbox.OutboxStore;
import org.springframework.stereotype.Component;

import java.util.List;

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
