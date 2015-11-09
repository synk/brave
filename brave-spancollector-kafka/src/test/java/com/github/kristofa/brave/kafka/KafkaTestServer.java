package com.github.kristofa.brave.kafka;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.TestZKUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import scala.collection.JavaConversions;
import scala.collection.mutable.Buffer;

import java.util.Collections;
import java.util.Properties;

public class KafkaTestServer {

    public class KafkaTestServerProperties {
        private final String zkConnectPropValue;
        private final String advertisedHostName;
        private final int advertisedPort;

        KafkaTestServerProperties(String zkConnectPropValue, String host, int port) {
            this.zkConnectPropValue = zkConnectPropValue;
            this.advertisedHostName = host;
            this.advertisedPort = port;
        }

        public String zkConnectPropValue() {
            return zkConnectPropValue;
        }

        public String advertisedHostName() {
            return advertisedHostName;
        }

        public int advertisedPort() {
            return advertisedPort;
        }
    }

    private KafkaServer server;
    private EmbeddedZookeeper zkServer;

    public KafkaTestServerProperties start(String topicName) {
        // Kafka setup
        zkServer = new EmbeddedZookeeper(TestZKUtils.zookeeperConnect());
        ZkClient zkClient = new ZkClient(zkServer.connectString(), 30000, 30000, ZKStringSerializer$.MODULE$);
        Properties props = TestUtils.createBrokerConfig(0, TestUtils.choosePort(), false);
        KafkaConfig config = new KafkaConfig(props);
        server = TestUtils.createServer(config, new MockTime());

        Buffer<KafkaServer> servers = JavaConversions.asScalaBuffer(Collections.singletonList(server));
        TestUtils.createTopic(zkClient, topicName, 1, 1, servers, new Properties());
        zkClient.close();
        TestUtils.waitUntilMetadataIsPropagated(servers, topicName, 0, 5000);
        return new KafkaTestServerProperties(props.getProperty("zookeeper.connect"), config.advertisedHostName(), config.advertisedPort());
    }

    public void stop() {
        server.shutdown();
        zkServer.shutdown();
    }
}
