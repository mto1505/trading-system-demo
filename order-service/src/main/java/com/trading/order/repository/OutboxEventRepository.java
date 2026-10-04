package com.trading.order.repository;

import com.trading.order.entity.OutboxEventEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface OutboxEventRepository extends JpaRepository<OutboxEventEntity, Long> {

    /**
     * FOR UPDATE SKIP LOCKED: nhiều pod relay chạy song song, mỗi pod khoá
     * một lô event khác nhau, KHÔNG chờ nhau, KHÔNG gửi trùng lô của nhau.
     * (Trùng vẫn có thể xảy ra khi crash giữa chừng - at-least-once by design.)
     */
    @Query(value = """
            SELECT * FROM ordersvc.outbox_event
            WHERE published_at IS NULL
            ORDER BY id
            LIMIT :n
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<OutboxEventEntity> lockUnpublishedBatch(@Param("n") int n);
}
