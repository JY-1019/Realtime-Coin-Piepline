package org.coin.models;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.json.JSONObject;

public class CustomTimeExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
        try {
            JSONObject jsonObject = new JSONObject((String) record.value());
            // 데이터에서 시간 정보를 추출합니다.
            long timestamp = jsonObject.getLong("timestamp");
            // 밀리초 단위로 변환하여 반환합니다.
            return timestamp;
        } catch (Exception e) {
            // 에러 발생 시 기본 타임스탬프를 반환합니다.
            return record.timestamp();
        }
    }
}