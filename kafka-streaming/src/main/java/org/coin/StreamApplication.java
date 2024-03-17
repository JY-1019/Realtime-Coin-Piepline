package org.coin;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.coin.models.CustomTimeExtractor;
import org.json.JSONException;
import org.json.JSONObject;

import java.time.Duration;
import java.util.Properties;


public class StreamApplication {

    private static String APPLICATION_NAME = "coin-application";
    private static String BOOTSTRAP_SERVERS = "broker2:9092";
    private static String SOURCE_UPBIT_TOPIC = "upbit";
    private static String SINK_UPBIT_TOPIC = "upbit-filter";
    private static String SOURCE_BYBIT_TOPIC = "bybit";
    private static String SINK_BYBIT_TOPIC = "bybit-filter";
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, CustomTimeExtractor.class);


        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(SOURCE_UPBIT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                .groupBy((key, value) -> {
                    try {
                        JSONObject jsonObject = new JSONObject(value);
                        String symbol = jsonObject.getString("symbol");
                        return symbol;
                    } catch (JSONException e){
                        return null;
                    }
                })
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(3L), Duration.ZERO))
                .reduce((value1, value2) -> value1)
                .toStream()
                .filter((key, value) -> key != null)
                .map((windowedKey, value) -> {
                    try {
                        long windowStartTime = windowedKey.window().start();
                        long windowEndTime = windowedKey.window().end();

                        JSONObject jsonObject = new JSONObject(value);
                        jsonObject.put("windowStartTime", windowStartTime);
                        jsonObject.put("windowEndTime", windowEndTime);

                        long eventTime = jsonObject.getLong("timestamp");
                        long currentTime = System.currentTimeMillis();
                        long latency = currentTime - eventTime;

                        jsonObject.put("latency-kafka", latency);

                        return KeyValue.pair(windowedKey.key(), jsonObject.toString());
                    } catch (JSONException e) {
                        return KeyValue.pair(windowedKey.key(), "");
                    }
                })
                .filter((key, value) -> value != "")
                .to(SINK_UPBIT_TOPIC);

        builder.stream(SOURCE_BYBIT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                .groupBy((key, value) -> {
                    try {
                        JSONObject jsonObject = new JSONObject(value);
                        String symbol = jsonObject.getString("symbol");
                        return symbol;
                    } catch (JSONException e){
                        return null;
                    }
                })
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(3L), Duration.ZERO))
                .reduce((value1, value2) -> value1)
                .toStream()
                .filter((key, value) -> key != null)
                .map((windowedKey, value) -> {
                    try {
                        long windowStartTime = windowedKey.window().start();
                        long windowEndTime = windowedKey.window().end();

                        JSONObject jsonObject = new JSONObject(value);
                        jsonObject.put("windowStartTime", windowStartTime);
                        jsonObject.put("windowEndTime", windowEndTime);

                        long eventTime = jsonObject.getLong("timestamp");
                        long currentTime = System.currentTimeMillis();
                        long latency = currentTime - eventTime;

                        jsonObject.put("latency-kafka", latency);

                        return KeyValue.pair(windowedKey.key(), jsonObject.toString());
                    } catch (JSONException e) {
                        return KeyValue.pair(windowedKey.key(), "");
                    }
                })
                .filter((key, value) -> value != "")
                .to(SINK_BYBIT_TOPIC);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

    }
}