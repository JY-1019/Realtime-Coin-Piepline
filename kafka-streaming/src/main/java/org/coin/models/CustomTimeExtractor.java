package org.coin.models;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.json.JSONObject;

public class CustomTimeExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
        try {
            JSONObject jsonObject = new JSONObject((String) record.value());
            long timestamp = jsonObject.getLong("timestamp");
            return timestamp;
        } catch (Exception e) {
            return record.timestamp();
        }
    }
}
