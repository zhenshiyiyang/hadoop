package kafka.kafka_producer;

public interface KafkaProperties{
	  final static String zkConnect = "219.216.64.251:2181,219.216.64.131:2181,219.216.64.130:2181";
	  final static  String groupId = "group1";
	  final static String topic = "mtopic";
	  final static String kafkaServerURL = "219.216.64.251,219.216.64.131,219.216.64.130";
	  final static int kafkaServerPort = 9092;
	  final static int kafkaProducerBufferSize = 64*1024;
	  final static int connectionTimeOut = 100000;
	  final static int reconnectInterval = 10000;
	  final static String clientId = "SimpleConsumerDemoClient";
}