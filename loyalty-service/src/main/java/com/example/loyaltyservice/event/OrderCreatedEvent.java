package com.example.loyaltyservice.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OrderCreatedEvent implements Serializable {
    @Builder.Default
    private String eventType = "order.created";
    private String orderId;
    private String customerId;
    private String productId;
    private Integer quantity;
}
