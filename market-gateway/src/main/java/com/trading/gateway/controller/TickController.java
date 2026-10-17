package com.trading.gateway.controller;

import com.trading.gateway.kafka.TickProducer;
import com.trading.common.model.TickEvent;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * REST endpoint cho nguồn dữ liệu ngoài bơm tick vào
 * (nếu nguồn là FIX/binary feed thì thay bằng adapter tương ứng).
 */
@CrossOrigin("*")
@RestController
@RequestMapping("/api/v1/ticks")
public class TickController {

    private final TickProducer producer;

    public TickController(TickProducer producer) {
        this.producer = producer;
    }

    @PostMapping
    public ResponseEntity<Void> ingest(@RequestBody TickEvent tick) {
        producer.send(tick);
        // 202 Accepted: đã nhận, xử lý bất đồng bộ (fire-and-forget)
        return ResponseEntity.accepted().build();
    }

    @PostMapping("/batch")
    public ResponseEntity<Void> ingestBatch(@RequestBody List<TickEvent> ticks) {
        ticks.forEach(producer::send);
        return ResponseEntity.accepted().build();
    }
}
