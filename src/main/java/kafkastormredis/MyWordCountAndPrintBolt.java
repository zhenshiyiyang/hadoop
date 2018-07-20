package kafkastormredis;

/**
 *
 * Created by hadoop on 2018/3/29.
 */
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

public class MyWordCountAndPrintBolt extends BaseBasicBolt{
    private Jedis jedis;
    private Map<String,String> wordCountMap = new HashMap<String,String>();

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        //连接redis---代表可以连接任何事物
        jedis = new Jedis("192.168.217.128",7006);
        super.prepare(stormConf,context);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String word = (String) input.getValueByField("word");
        Integer num = (Integer) input.getValueByField("num");
        //1、查看单词对应的value是否存在
        Integer integer = wordCountMap.get(word) == null ? 0 : Integer.parseInt(wordCountMap.get(word));
        if (integer == null || integer.intValue() == 0) {
            wordCountMap.put(word,num + "");
        } else {
            wordCountMap.put(word,(integer.intValue() + num) + "");
        }
        //2、保存到redis
        System.out.println(wordCountMap);
        //redis key wordcount:-->Map
        jedis.hmset("wordcount",wordCountMap);
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //todo 不需要定义输出的字段
    }
}
