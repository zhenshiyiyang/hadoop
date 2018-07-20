package mrToHbase;

/**
 * Created by hadoop on 2018/5/10.
 */
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class HbaseStoreMapper extends Mapper<Object, Text, Text, IntWritable>{
    private IntWritable one = new IntWritable(1);
    protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        context.write(value, one);
    }
}
