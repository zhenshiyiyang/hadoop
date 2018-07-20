package mrToHbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * Created by hadoop on 2018/5/10.
 */
public class HbaseStoreReducer extends TableReducer<Text, IntWritable, NullWritable>{
    Put put;
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        // 每行以制表符分隔 id, account, name, age
        String[] strs = key.toString().split(" ");

        //id为rowKey
        put = new Put(strs[0].getBytes());
        //account
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("account"), Bytes.toBytes(strs[1]));
        //name
        //put.addColumn(Bytes.toBytes("info"), Bytes.agetoBytes("name"), Bytes.toBytes(strs[2]));
        //
        //put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(strs[3]));
        System.out.println(strs[3]);
        context.write(NullWritable.get(), put);
    }
}
