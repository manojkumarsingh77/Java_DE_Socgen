package com.bank.retail.streaming.util;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * JsonUtil
 * --------
 * A single shared, reusable Jackson ObjectMapper.
 *
 * WHY a shared static instance instead of "new ObjectMapper()" everywhere?
 * ObjectMapper does internal caching/reflection work the first time it sees
 * a class. Creating a fresh one per message (e.g. inside a loop sending
 * thousands of Kafka messages) is a well-known performance foot-gun.
 * Jackson's ObjectMapper is thread-safe for read/write operations once
 * configured, so one static instance is both correct and fast.
 */
public final class JsonUtil {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private JsonUtil() {
        // utility class - never instantiated
    }

    /** Serializes any Java object (here: PaymentOrderEvent) into a JSON string. */
    public static String toJson(Object value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize object to JSON", e);
        }
    }
}
