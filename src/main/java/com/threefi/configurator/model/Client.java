package com.threefi.configurator.model;

/**
 * Created by thomaskwscott on 16/03/18.
 */
public class Client {

    private String type;
    private String clusterId;
    private String topic;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }



    public String getClusterId() {
        return clusterId;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }



    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
}
