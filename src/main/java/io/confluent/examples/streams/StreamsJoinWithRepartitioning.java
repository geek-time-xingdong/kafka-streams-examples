package io.confluent.examples.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;

import java.time.Duration;
import java.util.Properties;

public class StreamsJoinWithRepartitioning {

    public static void main(String[] args) {
        final Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "test-application");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        final StreamsBuilder builder = new StreamsBuilder();


        final KStream<String, String> streamOne = builder.stream("input-topic-one");
        final KStream<String, String> streamTwo = builder.stream("input-topic-two");

        final KStream<String, String> streamOneNewKey = streamOne.selectKey((k, v) -> v.substring(0, 5));
        final KStream<String, String> streamTwoNewKey = streamTwo.selectKey((k, v) -> v.substring(4, 9));


        streamOneNewKey.join(streamTwoNewKey,(v1, v2) -> v1+":"+v2, JoinWindows.of(Duration.ofMinutes(5))).to("joined-output");


        final Topology topology = builder.build(properties);
        // Could also be done via a log statement
        System.out.println(topology.describe());

        final KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);

        kafkaStreams.start();
    }
}