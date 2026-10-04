package com.trading.matching.repository;

import com.trading.matching.entity.OutboxEventEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface OutboxEventRepository extends JpaRepository<OutboxEventEntity, Long> {

    @Query(value = """
            SELECT * FROM matchsvc.outbox_event
            WHERE published_at IS NULL
            ORDER BY id
            LIMIT :n
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<OutboxEventEntity> lockUnpublishedBatch(@Param("n") int n);
}
