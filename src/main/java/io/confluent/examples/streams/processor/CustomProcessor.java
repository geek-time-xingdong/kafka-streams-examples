package io.confluent.examples.streams.processor;//package com.immomo.moaservice.recommend.stream;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.util.Objects;

/**
 * @author chengzhengzheng
 * @date 2020/11/5
 */
public class CustomProcessor implements Processor<String, UserHeartBeat> {
    private final String                               stateStoreName;
    private       KeyValueStore<String, UserHeartBeat> stateStore;
    private       ProcessorContext                     context;

    public CustomProcessor(String stateStoreName) {
        this.stateStoreName = stateStoreName;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        stateStore   = context.getStateStore(stateStoreName);
        context.schedule(Duration.ofSeconds(5), PunctuationType.WALL_CLOCK_TIME, new CustomPunctuator(context, stateStore));
        Objects.requireNonNull(stateStore, "State store can't be null");
    }

    @Override
    public void process(String key, UserHeartBeat value) {
        UserHeartBeat userHeartBeat = stateStore.get(value.getId());
        if (userHeartBeat == null) {
            stateStore.put(value.getId(), value);
        } else {
            userHeartBeat.update(value);
            stateStore.put(value.getId(), userHeartBeat);
        }
        context.commit();

    }

    @Override
    public void close() {

    }
}
