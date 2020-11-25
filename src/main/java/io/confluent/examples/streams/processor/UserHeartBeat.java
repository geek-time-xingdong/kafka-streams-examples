package io.confluent.examples.streams.processor;

import com.google.gson.Gson;

import java.io.Serializable;

/**
 * @author chengzhengzheng
 * @date 2020/11/5
 */
public class UserHeartBeat implements Serializable {
    private String id;
    private Long   time;
    private Long   times = 0L;

    public static void main(String[] args) {
        Gson   gson = new Gson();
        String s    = gson.toJson(new UserHeartBeat("222", 1L,1L));
        System.out.println(s);

    }

    @Override
    public String toString() {
        return "UserHeartBeats{" +
                "id='" + id + '\'' +
                ", time=" + time +
                ", times=" + times +
                '}';
    }

    public UserHeartBeat(String id, Long time, Long times) {
        this.id   = id;
        this.time = time;
        this.times = times;
    }

    public UserHeartBeat() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public void update(UserHeartBeat value) {
        this.time = value.getTime();
        this.times++;
    }

    public Long getTimes() {
        return times;
    }

    public void setTimes(Long times) {
        this.times = times;
    }
}
