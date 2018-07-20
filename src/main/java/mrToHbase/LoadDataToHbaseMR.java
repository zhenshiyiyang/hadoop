package mrToHbase;

/**
 * Created by hadoop on 2018/5/10.
 */
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class LoadDataToHbaseMR {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String tablename = "test";
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.217.128,192.168.217.129,192.168.217.130");
        //conf.set("hbase.zookeeper.property.clientPort", "2181");
        //conf.set("dfs.socket.timeout", "180000");
        Job job = Job.getInstance(conf, "hbasetest");
        job.setJarByClass(LoadDataToHbaseMR.class);

        job.setMapperClass(HbaseStoreMapper.class);
        job.setReducerClass(HbaseStoreReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass( TableOutputFormat.class);
        job.setOutputValueClass(Put.class);
        // 设置输入文件路径
        FileInputFormat.addInputPath(job, new Path("hdfs://192.168.217.128:9000/liuxin/hbaseInput"));

        job.setNumReduceTasks(1);
        TableMapReduceUtil.initTableReducerJob("test", HbaseStoreReducer.class, job,null,null,null,null,false);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
