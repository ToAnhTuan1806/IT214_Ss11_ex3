package com.example.loyaltyservice.consumer;

import com.example.loyaltyservice.event.OrderCreatedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class LoyaltyConsumer {

    @KafkaListener(
            topics = "storex-order-events",
            groupId = "loyalty-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeOrderCreated(
            @Payload OrderCreatedEvent event,
            @Header(value = KafkaHeaders.RECEIVED_PARTITION, required = false) Integer partition,
            @Header(value = KafkaHeaders.OFFSET, required = false) Long offset
    ) {
        log.info("================================");
        log.info("LOYALTY SERVICE");
        log.info("Received order.created");
        log.info("orderId: {}", event.getOrderId());
        log.info("customerId: {} | productId: {} | quantity: {}", 
                event.getCustomerId(), event.getProductId(), event.getQuantity());
        log.info("Metadata -> Partition: {} | Offset: {}", partition, offset);
        log.info("================================");
    }
}
