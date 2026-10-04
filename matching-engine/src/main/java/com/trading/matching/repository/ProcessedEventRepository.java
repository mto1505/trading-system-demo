package com.trading.matching.repository;

import com.trading.matching.entity.ProcessedEventEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface ProcessedEventRepository extends JpaRepository<ProcessedEventEntity, String> {

    /**
     * Atomic check-and-set bằng chính DB (không cần lock ứng dụng):
     * trả về 1 = lần đầu thấy id này -> xử lý; 0 = đã xử lý -> bỏ qua.
     * Chạy trong CÙNG transaction với việc ghi execution -> nguyên tử.
     */
    @Modifying
    @Query(value = """
            INSERT INTO matchsvc.processed_event (client_order_id, processed_at)
            VALUES (:id, now())
            ON CONFLICT (client_order_id) DO NOTHING
            """, nativeQuery = true)
    int insertIfAbsent(@Param("id") String clientOrderId);
}
