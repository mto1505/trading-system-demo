package com.trading.matching.repository;

import com.trading.matching.entity.ExecutionEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface ExecutionRepository extends JpaRepository<ExecutionEntity, Long> {
    List<ExecutionEntity> findByClientOrderId(String clientOrderId);
}
