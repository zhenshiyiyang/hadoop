package kafkastormredis;

/**
 * 这个Bolt模拟从kafkaSpout接收数据，并把数据信息发送给MyWordCountAndPrintBolt的过程。
 * Created by hadoop on 2018/3/29.
 */
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class MySplitBolt extends BaseBasicBolt{
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        //1、数据如何获取
        //如果StormTopologyDriver中的spout配置的是MyLocalFileSpout，则用的是declareOutputFields中的juzi这个key
        //byte[] juzi = (byte[]) input.getValueByField("juzi");
        //2、这里用这个是因为StormTopologyDriver这个里面的spout用的是KafkaSpout，而KafkaSpout中的declareOutputFields返回的是bytes，所以下面用bytes，这个地方主要模拟的是从kafka中获取数据
        byte[] juzi = (byte[]) input.getValueByField("bytes");
        //2、进行切割
        String[] strings = new String(juzi).split(" ");
        //3、发送数据
        for (String word : strings) {
            //Values对象帮我们生成一个list
            collector.emit(new Values(word,1));
        }
    }
    //判断渔船位置是否越界，越界则返回true，不越界则返回false
    public boolean judge(double lon,double lat){
        double lon_start = 117.583333;
        double lon_end = 122.25;
        double lat_start = 37.116667;
        double lat_end = 41;
        if(lon>lon_start && lon<lon_end && lat>lat_start && lat<lat_end){
             return false;
        }else{
             return true;
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word","num"));
    }
}
