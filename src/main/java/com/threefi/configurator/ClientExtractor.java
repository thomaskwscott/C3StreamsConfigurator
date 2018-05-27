package com.threefi.configurator;

import io.confluent.monitoring.record.Monitoring.MonitoringMessage;
import io.confluent.serializers.ProtoSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Created by thomaskwscott on 16/03/18.
 */
public class ClientExtractor {

    public static final Set<String> FOUND_CLIENTS = new HashSet<>();
    public static final int NO_ACTIVITY_TIMEOUT = 30000;
    public static final int CLIENT_SEARCH_TIMEOUT = 120000;
    private StreamsConfig streamingConfig;
    private final ProtoSerde<MonitoringMessage> monitoringMessageSerde=new ProtoSerde<MonitoringMessage>(MonitoringMessage.getDefaultInstance());;
    private KStreamBuilder builder;
    private static final String SEPERATOR=":";
    private static long LAST_CLIENT_UPDATE =System.currentTimeMillis();
    private static long START_TIME=System.currentTimeMillis();
    private KafkaStreams kafkaStreams;


    private void configure(String bootstrapServers,String monitoringTopic){

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "configurator-clientExtractor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamingConfig=new StreamsConfig(props);

        Serde<byte[]> byteArraySerde = Serdes.ByteArray();
        builder = new KStreamBuilder();
        KStream<byte[],byte[]> monitoringEvents = builder.stream(byteArraySerde, byteArraySerde, monitoringTopic);
        KStream<byte[], MonitoringMessage> deserializedEvents = monitoringEvents.mapValues(s -> monitoringMessageSerde.deserialize(s));
        deserializedEvents.foreach((key, value) -> updateFoundClients(value));


    }

    private void updateFoundClients(MonitoringMessage monitoringMessage) {
        String clientInfo = monitoringMessage.getClusterId() + (monitoringMessage.getClusterId().isEmpty()?"":SEPERATOR) +
                monitoringMessage.getClientType().toString() + (monitoringMessage.getClientType().toString().isEmpty()?"":SEPERATOR) +
                monitoringMessage.getTopic();
        if(!FOUND_CLIENTS.contains(clientInfo))
        {
            System.out.println("Found client:" + clientInfo);
            FOUND_CLIENTS.add(clientInfo);
            LAST_CLIENT_UPDATE = System.currentTimeMillis();
        }
        if(System.currentTimeMillis() - LAST_CLIENT_UPDATE > NO_ACTIVITY_TIMEOUT || System.currentTimeMillis() - START_TIME > CLIENT_SEARCH_TIMEOUT)
        {
            kafkaStreams.close(10000, TimeUnit.MILLISECONDS);
        }
    }

    public void start() {

        kafkaStreams = new KafkaStreams(builder, streamingConfig);
        System.out.println("Searching for clients for a maximum of " + CLIENT_SEARCH_TIMEOUT + "ms" );
        kafkaStreams.start();
        //Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

    public static void main(String[] args) throws InterruptedException {

        /*
        args;
        0 - boostrap server
        1 - monitoring topic
         */

        ClientExtractor extractor = new ClientExtractor();
        extractor.configure(args[0],args[1]);
        extractor.start();
        while (extractor.kafkaStreams.state().isRunning())
        {
            Thread.currentThread().sleep(1000);
        }
        System.out.println("Finished finding clients");

    }


}
