package com.threefi.configurator;

import io.confluent.monitoring.record.Monitoring.MonitoringMessage;
import io.confluent.serializers.ProtoSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
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
public class TestStreamsApp {


    private StreamsConfig streamingConfig;
    private KStreamBuilder builder;
    private KafkaStreams kafkaStreams;

    private void configure(String bootstrapServers,String inputTopic, String outputTopic1, String outputTopic2){

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "configurator-testStreamsApp");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(
                StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
                "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor");
        props.put(
                StreamsConfig.PRODUCER_PREFIX + ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor");

        streamingConfig=new StreamsConfig(props);

        Serde<byte[]> byteArraySerde = Serdes.ByteArray();
        Serde<String> stringSerde = Serdes.String();
        builder = new KStreamBuilder();
        KStream<byte[],String> inEvents = builder.stream(byteArraySerde, stringSerde, inputTopic);
        KStream<byte[], String>[] branches = inEvents.branch(
                (key, value) -> Math.random() > 0.5, /* first predicate  */
                (key, value) -> true                 /* third predicate  */
        );

        branches[0].mapValues(v -> v.getBytes()).to(outputTopic1);
        branches[1].mapValues(v -> v.getBytes()).to(outputTopic2);
    }


    public void start() {

        kafkaStreams = new KafkaStreams(builder, streamingConfig);
        System.out.println("Splitting data to 2 output topics");
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

    public static void main(String[] args) {

        /*
        args;
        0 - boostrap server
        1 - input topic
        2 - output topic 1
        3 - output topic 2

         */

        TestStreamsApp extractor = new TestStreamsApp();
        extractor.configure(args[0],args[1],args[2],args[3]);
        extractor.start();

    }


}
