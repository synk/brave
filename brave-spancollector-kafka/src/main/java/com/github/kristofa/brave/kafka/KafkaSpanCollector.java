package com.github.kristofa.brave.kafka;

import com.github.kristofa.brave.SpanCollector;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.zipkin.gen.Span;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

/**
 * SpanCollector which submits spans to Kafka using <a href="http://kafka.apache.org/documentation.html#producerapi">Kafka Producer api</a>.
 * <p>
 * Spans are sent to kafka as keyed messages: the key is the topic zipkin and the value is a TBinaryProtocol encoded Span.
 * </p>
 * <p>
 * It is advised to use the Kafka producer
 * in async mode (producer.type = async) which is part of the default settings.
 * Async mode will use batching which should increase performance. Have a look at
 * <a href="http://kafka.apache.org/documentation.html#producerconfigs">Producer api config options</a> for different
 * config options.
 * </p>
 */
public class KafkaSpanCollector implements SpanCollector, Closeable {

    private static final Logger LOGGER = Logger.getLogger(KafkaSpanCollector.class.getName());
    private static final Properties DEFAULT_PROPERTIES = new Properties();

    static {
        DEFAULT_PROPERTIES.put("request.required.acks", "0");
        DEFAULT_PROPERTIES.put("producer.type", "async");
        DEFAULT_PROPERTIES.put("serializer.class", "kafka.serializer.DefaultEncoder");
        DEFAULT_PROPERTIES.put("compression.codec", "1");
    }

    private static Properties defaultPropertiesWith(String metadataBrokerList) {
        Properties props = new Properties(DEFAULT_PROPERTIES);
        props.setProperty("metadata.broker.list", metadataBrokerList);
        return props;
    }

    private final Producer<byte[], byte[]> producer;
    private final ExecutorService executorService;
    private final SpanProcessingTask spanProcessingTask;
    private final Future<Integer> future;
    private final BlockingQueue<Span> queue;

    /**
     * Create a new instance with default configuration.
     *
     * @param metadataBrokerList Comma separated list of brokers for retrieving metadata and
     *                           bootstrapping producer. Like: host1:port1,host2:port2,...
     */
    public KafkaSpanCollector(String metadataBrokerList) {
        this(KafkaSpanCollector.defaultPropertiesWith(metadataBrokerList));
    }

    /**
     * KafkaSpanCollector.
     *
     * @param kafkaProperties    Configuration for Kafka producer. Essential configuration properties are:
     *                           metadata.broker.list, request.required.acks, producer.type, serializer.class. For a
     *                           full list of config options, see http://kafka.apache.org/documentation.html#producerconfigs.
     */
    public KafkaSpanCollector(Properties kafkaProperties) {
        producer = new Producer<>(new ProducerConfig(kafkaProperties));
        executorService = Executors.newSingleThreadExecutor();
        queue = new ArrayBlockingQueue<Span>(1000);
        spanProcessingTask = new SpanProcessingTask(queue, producer);
        future = executorService.submit(spanProcessingTask);
    }

    @Override
    public void collect(com.twitter.zipkin.gen.Span span) {
        if (!queue.offer(span)) {
            LOGGER.log(Level.WARNING, "Queue rejected span!");
        }
    }

    @Override
    public void addDefaultAnnotation(String key, String value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        spanProcessingTask.stop();
        try {
            Integer nrProcessedSpans = future.get(6000, TimeUnit.MILLISECONDS);
            LOGGER.info("SpanProcessingTask processed " + nrProcessedSpans + " spans.");
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Exception when waiting for SpanProcessTask to finish.", e);
        }
        executorService.shutdown();
        producer.close();
        LOGGER.info("KafkaSpanCollector closed.");
    }
}