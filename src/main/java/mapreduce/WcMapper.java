package mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

/**
 * Created by hadoop on 2018/4/22.
 */
public class WcMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = StringUtils.split(value.toString(), ' ');
        for (String w : words) {
            context.write(new Text(w), new IntWritable(1));
        }
    }
}