package com.retail.model;
import java.io.Serializable;
public class OrderEvent implements Serializable {
 public String orderId; public String customerId; public String productId; public double amount; public String eventTime;
 public OrderEvent(){}
}
