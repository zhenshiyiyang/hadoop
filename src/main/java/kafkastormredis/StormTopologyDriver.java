package kafkastormredis;

/**
 *
 * 这个Driver使Kafka、strom、redis进行串联起来。
 *
 * 这个代码执行前需要创建kafka的topic,创建代码如下：
 * [root@hadoop1 kafka]# bin/kafka-topics.sh --create --zookeeper hadoop11:2181 --replication-factor 1 -partitions 3 --topic wordCount
 *
 * 接着还要向kafka中传递数据，打开一个shell的producer来模拟生产数据
 * [root@hadoop1 kafka]# bin/kafka-console-producer.sh --broker-list hadoop1:9092 --topic wordCount
 * 接着输入数据
 * Created by hadoop on 2018/3/29.
 */
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.topology.TopologyBuilder;

public class StormTopologyDriver {
    public static void main(String[] args) throws Exception {
        //1、准备任务信息
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("KafkaSpout",new KafkaSpout(new SpoutConfig(new ZkHosts("192.168.217.128:2181,192.168.217.129:2181,192.168.217.130:2181"),"wordCount","","hadoop")),12);
        topologyBuilder.setBolt("bolt1",new MySplitBolt(),4).shuffleGrouping("KafkaSpout");
        System.out.println("liuxin");
        topologyBuilder.setBolt("bolt2",new MyWordCountAndPrintBolt(),2).shuffleGrouping("bolt1");

        //2、任务提交z
        //提交给谁？提交内容
        Config config = new Config();
        //本地模式调试
        config.setDebug(true);
        config.setNumWorkers(2);
        StormTopology stormTopology = topologyBuilder.createTopology();

        //本地模式
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("wordcount",config,stormTopology);
        //集群模式
        //StormSubmitter.submitTopology("wordcount",config,stormTopology)；
        localCluster.killTopology("wordcount");
        localCluster.shutdown();
    }
}
