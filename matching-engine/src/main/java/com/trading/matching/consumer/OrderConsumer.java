package com.trading.matching.consumer;

import com.trading.common.model.OrderResult;
import com.trading.matching.service.MatchingService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderConsumer {

    private final MatchingService matchingService;

    public OrderConsumer(MatchingService matchingService) {
        this.matchingService = matchingService;
    }

    @KafkaListener(topics = "stock.orders",
                   groupId = "matching-engine-group",
                   containerFactory = "orderListenerFactory")
    public void onOrder(OrderResult order) {
        matchingService.process(order);
        // Offset commit SAU khi transaction DB commit (listener trả về) ->
        // crash giữa chừng chỉ gây redelivery, dedupe lo phần còn lại.
    }
}
