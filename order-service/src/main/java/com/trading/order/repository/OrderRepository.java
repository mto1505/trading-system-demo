package com.trading.order.repository;

import com.trading.order.entity.OrderEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface OrderRepository extends JpaRepository<OrderEntity, Long> {
    Optional<OrderEntity> findByClientOrderId(String clientOrderId);

    /*
     * Phân trang là BẮT BUỘC cho endpoint liệt kê, không phải tuỳ chọn.
     * Bảng orders giữ MỌI lệnh kể cả REJECTED để phục vụ audit, nên nó chỉ có
     * tăng. Một `findAll()` không giới hạn sẽ kéo cả bảng vào heap 336Mi của pod
     * và giết nó bằng OOM - và sự cố đó chỉ xảy ra ở môi trường có dữ liệu thật,
     * không bao giờ lộ ra lúc dev.
     */
    Page<OrderEntity> findAllByStatus(String status, Pageable pageable);

    Page<OrderEntity> findAllByAccountId(String accountId, Pageable pageable);

    Page<OrderEntity> findAllByAccountIdAndStatus(String accountId, String status, Pageable pageable);
}
