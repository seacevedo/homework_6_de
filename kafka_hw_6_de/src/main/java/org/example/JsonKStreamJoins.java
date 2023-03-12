package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.*;
import org.example.customserdes.CustomSerdes;
import org.example.data.Ride;

import java.time.Duration;
import java.util.Properties;
public class JsonKStreamJoins {
    private Properties props = new Properties();

    public JsonKStreamJoins() {
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "pkc-lgk0v.us-west1.gcp.confluent.cloud:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='"+Secrets.KAFKA_CLUSTER_KEY+"' password='"+Secrets.KAFKA_CLUSTER_SECRET+"';");
        props.put("sasl.mechanism", "PLAIN");
        props.put("client.dns.lookup", "use_all_dns_ips");
        props.put("session.timeout.ms", "45000");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka_tutorial.kstream.joined.rides.pickuplocation.v1");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    }

    public Topology createTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, Ride> ridesGreen = streamsBuilder.stream("green_rides", Consumed.with(Serdes.String(), CustomSerdes.getSerde(Ride.class)));
        KStream<String, Ride> ridesFHV = streamsBuilder.stream("rides_fhv", Consumed.with(Serdes.String(), CustomSerdes.getSerde(Ride.class)));
        KStream<String, Ride> allRides = ridesGreen.merge(ridesFHV);

        var puLocationCount = allRides.groupByKey().count().toStream();

        puLocationCount.to("rides_all", Produced.with(Serdes.String(), Serdes.Long()));

        //allRides.groupByKey().count().toStream().to("rides_all", Produced.with(Serdes.String(), CustomSerdes.getSerde(Ride.class)));


        return streamsBuilder.build();
    }

    public void joinRidesPickupLocation() throws InterruptedException {
        var topology = createTopology();
        var kStreams = new KafkaStreams(topology, props);

        kStreams.setUncaughtExceptionHandler(exception -> {
            System.out.println(exception.getMessage());
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
        });
        kStreams.start();
        while (kStreams.state() != KafkaStreams.State.RUNNING) {
            System.out.println(kStreams.state());
            Thread.sleep(1000);
        }
        System.out.println(kStreams.state());
        Runtime.getRuntime().addShutdownHook(new Thread(kStreams::close));

    }

    public static void main(String[] args) throws InterruptedException {
        var object = new JsonKStreamJoins();
        object.joinRidesPickupLocation();
    }
}
