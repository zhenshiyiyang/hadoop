package kafkastormhbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

/**
 * Created by hadoop on 2018/7/1.
 */
//创建需要的hbase表
public class PrepareHbase {
    public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "master,node1,node2");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor tableDescriptor = new HTableDescriptor("Bound");
        tableDescriptor.addFamily(new HColumnDescriptor("fishery"));
        admin.createTable(tableDescriptor);
    }
}