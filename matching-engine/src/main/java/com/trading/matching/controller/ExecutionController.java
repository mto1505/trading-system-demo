package com.trading.matching.controller;

import com.trading.matching.entity.ExecutionEntity;
import com.trading.matching.repository.ExecutionRepository;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/** Tra cứu kết quả khớp - dùng để verify idempotency (1 clientOrderId = đúng 1 execution). */
@RestController
@RequestMapping("/api/v1/executions")
public class ExecutionController {

    private final ExecutionRepository repository;

    public ExecutionController(ExecutionRepository repository) {
        this.repository = repository;
    }

    @GetMapping("/{clientOrderId}")
    public List<ExecutionEntity> byClientOrderId(@PathVariable String clientOrderId) {
        return repository.findByClientOrderId(clientOrderId);
    }
}
