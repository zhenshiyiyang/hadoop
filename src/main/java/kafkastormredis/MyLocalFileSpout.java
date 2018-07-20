package kafkastormredis;

/**
 * 这个类是模拟从文件中读取数据的代码。在本案例的strom + kafka + redis的案例中将用不到。
 * Created by hadoop on 2018/3/29.
 */
import org.apache.commons.lang.StringUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MyLocalFileSpout extends BaseRichSpout{
    private SpoutOutputCollector collector;
    private BufferedReader bufferedReader;

    /**
     * 初始化方法
     * @param map
     * @param context
     * @param collector
     */
    public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        try {
            this.bufferedReader = new BufferedReader(new FileReader(new File("E:/wordcount/input/1.txt")));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Strom实时计算的特性就是对数据一条一条的处理
     * while(true) {
     *     this.nextTuple();
     * }
     */
    public void nextTuple() {
        //每被调用一次就会发送一条数据出去
        try {
            String line = bufferedReader.readLine();
            if (StringUtils.isNotBlank(line)) {
                List<Object> arrayList = new ArrayList<Object>();
                arrayList.add(line);
                collector.emit(arrayList);
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("juzi"));
    }

}
