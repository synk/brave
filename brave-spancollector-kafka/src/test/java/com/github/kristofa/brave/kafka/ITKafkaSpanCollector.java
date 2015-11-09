package com.github.kristofa.brave.kafka;

import com.github.kristofa.brave.SpanCollector;
import com.twitter.zipkin.gen.Span;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.Assert.assertEquals;

public class ITKafkaSpanCollector {

    private final static String TOPIC = "zipkin";
    private KafkaTestServer kafkaServer;
    private KafkaTestServer.KafkaTestServerProperties serverProperties;

    @Before
    public void setup() {
        kafkaServer = new KafkaTestServer();
        serverProperties = kafkaServer.start(TOPIC);
    }

    @After
    public void tearDown() {
        kafkaServer.stop();
    }

    @Test
    public void submitSingleSpan() throws TException {
        SpanCollector kafkaCollector = new KafkaSpanCollector(serverProperties.advertisedHostName()+":"+serverProperties.advertisedPort());
        Span span = span(1l, "test_kafka_span");
        kafkaCollector.collect(span);
        kafkaCollector.close();
        List<Span> spans = getCollectedSpans(serverProperties.zkConnectPropValue());
        assertEquals(1, spans.size());
        assertEquals(span, spans.get(0));
    }

    @Test
    public void submitMultipleSpansInParallel() throws InterruptedException, ExecutionException, TimeoutException, TException {
        SpanCollector kafkaCollector = new KafkaSpanCollector(serverProperties.advertisedHostName()+":"+serverProperties.advertisedPort());
        Callable<Void> spanProducer1 = new Callable<Void>() {

            @Override
            public Void call() throws Exception {
                for(int i=1; i<=200; i++)
                {
                    kafkaCollector.collect(span(i, "producer1_" + i));
                }
                return null;
            }
        };

        Callable<Void> spanProducer2 = new Callable<Void>() {

            @Override
            public Void call() throws Exception {
                for(int i=1; i<=200; i++)
                {
                    kafkaCollector.collect(span(i, "producer2_"+i));
                }
                return null;
            }
        };

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        Future<Void> future1 = executorService.submit(spanProducer1);
        Future<Void> future2 = executorService.submit(spanProducer2);

        future1.get(2000, TimeUnit.MILLISECONDS);
        future2.get(2000, TimeUnit.MILLISECONDS);

        List<Span> spans = getCollectedSpans(serverProperties.zkConnectPropValue());
        assertEquals(400, spans.size());
        kafkaCollector.close();
    }

    private List<Span> getCollectedSpans(String zkConnectPropertyValue) throws TException {
        Properties consumerProps = new Properties();
        consumerProps.put("zookeeper.connect", zkConnectPropertyValue);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "testing.group");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "smallest");
        // Prevents that our ConsumerIterator blocks forever.
        consumerProps.put("consumer.timeout.ms", "5000");
        ConsumerConnector connector =
                kafka.consumer.Consumer.createJavaConsumerConnector(new kafka.consumer.ConsumerConfig(consumerProps));
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(TOPIC, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> streams = connector.createMessageStreams(topicCountMap);

        TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
        ConsumerIterator<byte[], byte[]> it = streams.get(TOPIC).get(0).iterator();
        List<Span> spans = new ArrayList<>();
        try {
            while (it.hasNext()) {
                Span span = new Span();
                deserializer.deserialize(span, it.next().message());
                spans.add(span);
            }
        } catch(ConsumerTimeoutException e)
        {
            // Nothing to do here. Timeout expected when all messages are submitted.
        }
        return spans;
    }

    private Span span(long traceId, String spanName) {
        final Span span = new Span();
        span.setId(traceId);
        span.setTrace_id(traceId);
        span.setName(spanName);
        return span;
    }
}
