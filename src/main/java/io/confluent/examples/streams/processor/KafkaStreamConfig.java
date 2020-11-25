package io.confluent.examples.streams.processor;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import static org.apache.kafka.clients.consumer.ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.*;

/**
 * @author chengzhengzheng
 * @date 2020/11/4
 */
@Configuration
@EnableKafka
public class KafkaStreamConfig {
    //When an application is updated, the `application.id` should be changed unless you want to reuse the existing data in internal topics and state stores. For example, you could embed the version information within `application.id`, as `my-app-v1.0.0` and `my-app-v1.0.2`.
    private static final String APPLICATION_ID    = "kafka-marry-heart-beat-v1.0";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    @Bean
    public KafkaStreamsConfiguration kafkaStreamsConfiguration() {
        ImmutableMap<String, Object> config = ImmutableMap.<String, Object>builder().
                put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID).
                put(DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName()).
                put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS).
                put(NUM_STREAM_THREADS_CONFIG, 1).
                put(consumerPrefix(SESSION_TIMEOUT_MS_CONFIG), 30000).
                put(consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest").
                put(DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class).
                put(REPLICATION_FACTOR_CONFIG, 1).
                put(CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024L).
                put(topicPrefix(TopicConfig.RETENTION_MS_CONFIG), Integer.MAX_VALUE).
                put(producerPrefix(ProducerConfig.ACKS_CONFIG), "all").
                put(producerPrefix(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG), 2147483647).
                put(producerPrefix(ProducerConfig.MAX_BLOCK_MS_CONFIG), 9223372036854775807L).
                build();
        return new KafkaStreamsConfiguration(config);
    }

    @Bean("heartBeatStreamsBuilderFactoryBean")
    @Primary
    public StreamsBuilderFactoryBean streamsBuilderFactoryBean(KafkaStreamsConfiguration kafkaStreamsConfiguration) throws Exception {
        StreamsBuilderFactoryBean streamsBuilderFactoryBean = new StreamsBuilderFactoryBean(kafkaStreamsConfiguration);
        streamsBuilderFactoryBean.afterPropertiesSet();
        streamsBuilderFactoryBean.setInfrastructureCustomizer(new HeartBeatInfrastructureCustomizer(KafkaConstants.HEART_BEAT_TOPIC));
        streamsBuilderFactoryBean.setCloseTimeout(10);
        return streamsBuilderFactoryBean;
    }

}
