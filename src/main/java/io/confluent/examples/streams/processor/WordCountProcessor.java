package io.confluent.examples.streams.processor;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * @author chengzhengzheng
 * @date 2020/11/8
 */
public class WordCountProcessor implements Processor<String, String> {
    private ProcessorContext               context;
    private KeyValueStore<String, Integer> kvStore;

    @Override

    public void init(ProcessorContext context) {
        this.context = context;
        this.kvStore = (KeyValueStore<String, Integer>) context.getStateStore("Counts");
        this.context.schedule(Duration.ofMillis(1000), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
            KeyValueIterator<String, Integer> iterator = kvStore.all();
            iterator.forEachRemaining(entry -> {
                context.forward(entry.key, entry.value);
                kvStore.delete(entry.key);
            });
            context.commit();
        });
    }

    @Override
    public void process(String key, String value) {
        Stream.of(value.toLowerCase().split(" ")).forEach(word -> {
            Optional<Integer> counts = Optional.ofNullable(kvStore.get(word));
            Integer           count  = counts.map(worldCount -> worldCount + 1).orElse(1);
            kvStore.put(word, count);
        });
    }

    @Override
    public void close() {
        kvStore.close();
    }
}
