package com.threefi.configurator;

import com.threefi.configurator.model.Client;
import io.confluent.monitoring.record.Monitoring;
import io.confluent.monitoring.record.Monitoring.MonitoringMessage;
import io.confluent.serializers.ProtoSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by thomaskwscott on 16/03/18.
 */
public class MessageRegenerator {

    private String clusterName;
    private List<Client> sources = new ArrayList<>();
    private List<Client> sinks = new ArrayList<>();
    private Map<String,String> topicMappings = new HashMap<>();

    private StreamsConfig streamingConfig;
    private final ProtoSerde<MonitoringMessage> monitoringMessageSerde=new ProtoSerde<MonitoringMessage>(MonitoringMessage.getDefaultInstance());;
    private KStreamBuilder builder;
    private static final String SEPERATOR=":";
    private KafkaStreams kafkaStreams;

    private void configure(String bootstrapServers,String monitoringTopic,String clusterName,String sources,String sinks, String topicMappings){

        this.clusterName = clusterName;

        for (String source : sources.split(",")) {
             this.sources.add(clientFromString(source));
         }
        for (String sink : sinks.split(",")) {
            this.sinks.add(clientFromString(sink));
        }

        for(String topicMapping : topicMappings.split(","))
        {
            String[] mapping = topicMapping.split("=");
            this.topicMappings.put(mapping[0],mapping[1]);
        }

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "configurator-regenerator");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamingConfig=new StreamsConfig(props);


        Serde<byte[]> byteArraySerde = Serdes.ByteArray();
        builder = new KStreamBuilder();
        KStream<byte[],byte[]> monitoringEvents = builder.stream(byteArraySerde, byteArraySerde, monitoringTopic);

        // deserialize
        KStream<byte[], MonitoringMessage> deserializedEvents = monitoringEvents.mapValues(s -> monitoringMessageSerde.deserialize(s));

        // filter for the clients we are interested in
        KStream<byte[], MonitoringMessage> sourceInputEvents = deserializedEvents.filter((key,value) -> isSourceInputMessage(value));
        KStream<byte[], MonitoringMessage> sinkInputEvents = deserializedEvents.filter((key,value) -> isSinkInputMessage(value));

        // convert to source or sink
        KStream<byte[], MonitoringMessage> sourceOutputEvents = sourceInputEvents.mapValues(value -> makeSourceMessage(value));
        KStream<byte[], MonitoringMessage> sinkOutputEvents = sinkInputEvents.mapValues(value -> makeSinkMessage(value));

        // serialize
        KStream<byte[], byte[]> serializedSourceOutputEvents = sourceOutputEvents.mapValues(value -> monitoringMessageSerde.serialize(value));
        KStream<byte[], byte[]> serializedSinkOutputEvents = sinkOutputEvents.mapValues(value -> monitoringMessageSerde.serialize(value));

        // replay them back to the monitoring topic
        serializedSinkOutputEvents.to(monitoringTopic);
        serializedSourceOutputEvents.to(monitoringTopic);

    }

    private boolean isSourceInputMessage(MonitoringMessage message) {
        for(Client client : sources)
        {
            if(client.getClusterId().equals(message.getClusterId()) &&
                    client.getTopic().equals(message.getTopic()) &&
                    client.getType().equals(message.getClientType().toString())) {
                System.out.println("Found source input message: " + message.getClusterId() + SEPERATOR + message.getClientType() + SEPERATOR + message.getTopic());
                return true;
            }
        }
        return false;
    }

    private boolean isSinkInputMessage(MonitoringMessage message) {
        for(Client client : sinks)
        {
            if(client.getClusterId().equals(message.getClusterId()) &&
                    client.getTopic().equals(message.getTopic()) &&
                    client.getType().equals(message.getClientType().toString())) {
                System.out.println("Found sink input message: " + message.getClusterId() + SEPERATOR + message.getClientType() + SEPERATOR + message.getTopic());
                return true;
            }
        }
        //System.out.println("rejecting input message: " + message.getClusterId() + SEPERATOR + message.getClientType() + SEPERATOR + message.getTopic());
        return false;
    }

    private MonitoringMessage makeSourceMessage(MonitoringMessage message)
    {
        // source messages are producers
        MonitoringMessage.Builder sourceMessage = MonitoringMessage.newBuilder(message)
                .setClientType(Monitoring.ClientType.PRODUCER)
                .setClientTypeValue(Monitoring.ClientType.PRODUCER_VALUE)
                .setClusterId(clusterName)
                .setClusterName(clusterName)
                .setSession(Double.toString(Math.random()))
                .setTopic(mapTopic(message.getTopic()));
        return sourceMessage.build();
    }


    private MonitoringMessage makeSinkMessage(MonitoringMessage message)
    {
        /*
        really wanted to do something with latency here but as the producer interceptor makes latency
        0 when sending the monitoring messages there's nothing we can do
         */
        // sink messages are consumers

        System.out.println("Sink message count: " + message.getCount());
        MonitoringMessage.Builder sinkMessage = MonitoringMessage.newBuilder(message)
                .setClientType(Monitoring.ClientType.CONSUMER)
                .setClientTypeValue(Monitoring.ClientType.CONSUMER_VALUE)
                .setClusterId(clusterName)
                .setClusterName(clusterName)
                .setGroup(clusterName)
                .setClientId(clusterName)
                .setSession(Double.toString(Math.random()))
                .setTopic(mapTopic(message.getTopic()));
        return sinkMessage.build();
    }

    private String mapTopic(String inTopic)
    {
        if(topicMappings.keySet().contains(inTopic)) {
            //System.out.println("Mapped " + inTopic + " to " +topicMappings.get(inTopic));
            return topicMappings.get(inTopic);
        }
        return inTopic;
    }


    private Client clientFromString(String in)
    {
        String[] parts = in.split(SEPERATOR);
        Client client  = new Client();
        client.setClusterId(parts[0]);
        client.setType(parts[1]);
        client.setTopic(parts[2]);

        return client;

    }


    public void start() {

        kafkaStreams = new KafkaStreams(builder, streamingConfig);
        System.out.println("Replaying messages");
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

    public static void main(String[] args) {

        /*
         args format:
             2: created cluster name
             3: sources as comma seperated list of output from clientExtractor
             4: sinks as comma seperated list of output from clientExtractor
             5: topic mapping as comma seperated topic1=topic2
        */

        MessageRegenerator regenerator = new MessageRegenerator();
        regenerator.configure(args[0],args[1],args[2],args[3],args[4],args[5]);


        regenerator.start();

    }


}
