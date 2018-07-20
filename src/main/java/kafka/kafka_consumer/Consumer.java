package kafka.kafka_consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;


public class Consumer extends Thread {
  @SuppressWarnings("deprecation")
  private final ConsumerConnector consumer;
  private final String topic;
  private final String name;
  
  public Consumer(String name,String topic){
    consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
            createConsumerConfig());
    this.topic = topic;
    this.name = name;
  }

  @SuppressWarnings("deprecation")
  private static ConsumerConfig createConsumerConfig(){
    Properties props = new Properties();
    props.put("zookeeper.connect", KafkaProperties.zkConnect);
    props.put("group.id", KafkaProperties.groupId);
    props.put("zookeeper.session.timeout.ms", "60000");
    props.put("zookeeper.sync.time.ms", "200");
    props.put("auto.commit.interval.ms", "1000");
    //每次最少接收的字节数，默认是1
    //props.put("fetch.min.bytes", "1024");
    //每次最少等待时间，默认是100
    //props.put("fetch.wait.max.ms", "600000");
    return new ConsumerConfig(props);
  }
 
  @SuppressWarnings("deprecation")
  public void run() {
    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(topic, new Integer(1));
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
    KafkaStream<byte[], byte[]> stream =  consumerMap.get(topic).get(0);
    ConsumerIterator<byte[], byte[]> it = stream.iterator();
    while(it.hasNext()){
        System.out.println(new String(it.next().message()));
    }
  }
}