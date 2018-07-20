package kafka.kafka_consumer;

public class KafkaConsumerDemo implements KafkaProperties
{
  public static void main(String[] args){
    //Consumer1
    Consumer consumerThread1 = new Consumer("Consumer1", topic);

    consumerThread1.start();
  }
}