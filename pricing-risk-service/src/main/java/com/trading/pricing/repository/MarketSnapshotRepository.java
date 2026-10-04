package com.trading.pricing.repository;

import com.trading.pricing.model.MarketSnapshotEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface MarketSnapshotRepository extends JpaRepository<MarketSnapshotEntity, Long> {

    @Query(value = """
            SELECT * FROM market_snapshot
            WHERE symbol = :symbol
            ORDER BY snap_time DESC LIMIT :limit
            """, nativeQuery = true)
    List<MarketSnapshotEntity> findRecent(@Param("symbol") String symbol, @Param("limit") int limit);
}
