package io.confluent.examples.streams.processor;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.kafka.config.KafkaStreamsInfrastructureCustomizer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;

/**
 * @author chengzhengzheng
 * @date 2020/11/5
 */
public class HeartBeatInfrastructureCustomizer implements KafkaStreamsInfrastructureCustomizer {
    private final        String                      inputTopic;
    private static final String                      HEART_BEAT_K_TABLE = "heart-beat-k-table";
    private static final Serde<String>               KEY_SERDE          = Serdes.String();
    private static final Serde<UserHeartBeat>        VALUE_SERDE        = new JsonSerde<>(UserHeartBeat.class).ignoreTypeHeaders();
    private static final Deserializer<String>        KEY_JSON_DE        = new JsonDeserializer<>(String.class);
    private static final Deserializer<UserHeartBeat> VALUE_JSON_DE      = new JsonDeserializer<>(UserHeartBeat.class).ignoreTypeHeaders();

    public HeartBeatInfrastructureCustomizer(String inputTopic) {
        this.inputTopic = inputTopic;
    }

    @Override
    public void configureBuilder(StreamsBuilder builder) {
        StoreBuilder<KeyValueStore<String, UserHeartBeat>> stateStoreBuilder = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(HEART_BEAT_K_TABLE), KEY_SERDE, VALUE_SERDE);
        builder.build().addSource("Source", KEY_JSON_DE, VALUE_JSON_DE, inputTopic).
                addProcessor("Process", () -> new CustomProcessor(HEART_BEAT_K_TABLE), "Source").
                addStateStore(stateStoreBuilder, "Process");
    }

}
