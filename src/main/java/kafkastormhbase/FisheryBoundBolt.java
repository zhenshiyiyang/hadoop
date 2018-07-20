package kafkastormhbase;

/**
 * 这个Bolt模拟从kafkaSpout接收数据，并把数据信息发送给MyWordCountAndPrintBolt的过程。
 * Created by hadoop on 2018/3/29.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class FisheryBoundBolt extends BaseBasicBolt{
    private Connection connection;
    private Table table;
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "master,node1,node2");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            connection = ConnectionFactory.createConnection(config);
            //示例都是对同一个table进行操作，因此直接将Table对象的创建放在了prepare，在bolt执行过程中可以直接重用。
            table = connection.getTable(TableName.valueOf("Bound"));
        } catch (IOException e) {
            //do something to handle exception
        }
    }
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        //1、数据如何获取
        //如果StormTopologyDriver中的spout配置的是MyLocalFileSpout，则用的是declareOutputFields中的juzi这个key
        //byte[] juzi = (byte[]) input.getValueByField("juzi");
        //2、这里用这个是因为StormTopologyDriver这个里面的spout用的是KafkaSpout，而KafkaSpout中的declareOutputFields返回的是bytes，所以下面用bytes，这个地方主要模拟的是从kafka中获取数据
        byte[] juzi = (byte[]) input.getValueByField("bytes");
        //2、进行切割
        String[] strings = new String(juzi).split(",");
        //3、发送数据
        int lon = Integer.parseInt(strings[1]);
        int lat = Integer.parseInt(strings[2]);
        if(judge(lon,lat)){
            //collector.emit(new Values(strings[1],strings[7],strings[8]));
            try {
                SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");//设置日期格式
                String date = df.format(new Date());// new Date()为获取当前系统时间，也可使用当前时间戳
                Put put = new Put(Bytes.toBytes(date+strings[0]));
                //将被计数的单词写入cf:words列
                put.addColumn(Bytes.toBytes("fishery"), Bytes.toBytes("lon"), Bytes.toBytes(lon));
                //将单词的计数写入cf:counts列
                put.addColumn(Bytes.toBytes("fishery"), Bytes.toBytes("lat"), Bytes.toBytes(lat));
                table.put(put);
            } catch (IOException e) {
                //do something to handle exception
            }
        }
    }
    @Override
    public void cleanup() {
        //关闭table
        try {
            if(table != null) table.close();
        } catch (Exception e){
            //do something to handle exception
        } finally {
            //在finally中关闭connection
            try {
                connection.close();
            } catch (IOException e) {
                //do something to handle exception
            }
        }
    }
    //判断渔船位置是否越界，越界则返回true，不越界则返回false
    public boolean judge(double lon,double lat){
        GraphicalMain graphicalMain = new GraphicalMain();
        return graphicalMain.isPointInPolygon(lon,lat);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //declarer.declare(new Fields("word","count"));
    }
}
