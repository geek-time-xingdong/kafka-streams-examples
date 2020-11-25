package io.confluent.examples.streams.processor;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;


/**
 * @author chengzhengzheng
 * @date 2020/11/5
 */
public class CustomPunctuator implements Punctuator {

    private final ProcessorContext context;

    private final KeyValueStore<String, UserHeartBeat> stateStore;


    public CustomPunctuator(ProcessorContext context, KeyValueStore<String, UserHeartBeat> stateStore) {
        this.context    = context;
        this.stateStore = stateStore;
    }

    @Override
    public void punctuate(long timestamp) {
        System.out.println("Punctuator started.");
        KeyValueIterator<String, UserHeartBeat> iter = stateStore.all();
        while (iter.hasNext()) {
            KeyValue<String, UserHeartBeat> entry = iter.next();
            System.out.println(entry.key+":"+entry.value);
        }
        iter.close();
        // commit the current processing progress
        context.commit();

    }
}
