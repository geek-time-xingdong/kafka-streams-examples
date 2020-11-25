package io.confluent.examples.streams.processor;

import com.google.gson.Gson;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.jvnet.hk2.annotations.Service;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import java.util.ArrayList;
import java.util.List;

/**
 * @author ceyhunuzunoglu
 */
@Service
public class StateStoreQueryService {

  private final static String stateStoreName = "heart-beat-k-table";

  private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

  public StateStoreQueryService(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
    this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
  }

  public List<UserHeartBeat> getAllListeners() {

    List<UserHeartBeat> listeners = new ArrayList<>();
    System.out.println(getReadOnlyStore().all().hasNext());
    getReadOnlyStore().all().forEachRemaining(keyValue -> listeners.add(keyValue.value));
    return listeners;
  }

  public String getListenerSongs(String listenerId) {
    UserHeartBeat userHeartBeat = getReadOnlyStore().get(listenerId);
    return new Gson().toJson(userHeartBeat);
  }


  private ReadOnlyKeyValueStore<String, UserHeartBeat> getReadOnlyStore() {
    return streamsBuilderFactoryBean.getKafkaStreams().store(stateStoreName, QueryableStoreTypes.keyValueStore());
  }
}
