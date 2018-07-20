package mapreduce.tongweiWord;


import org.apache.derby.iapi.util.StringUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by hadoop on 2018/6/2.
 */
public class JobMain {
    public static void main(String[] args) throws Exception{
        Configuration config = new Configuration();
        config.set("fs.defaultFS","hdfs://219.216.64.251:9000");
        config.set("yarn.resourcemanager.hostname","219.216.64.251");
        try {
            FileSystem fs = FileSystem.get(config);
            Job job = Job.getInstance(config);
            job.setJarByClass(JobMain.class);
            job.setJobName("tongwei");
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);
            job.setMapperClass(wordMapper.class);
            job.setReducerClass(wordReducer.class);
            FileInputFormat.addInputPath(job,new Path("/liuxin/tongwei"));
            Path outPath = new Path("/liuxin/tongweiOut");
            if (fs.exists(outPath)) {
                fs.delete(outPath, true);
            }
            FileOutputFormat.setOutputPath(job,outPath);
            boolean f = job.waitForCompletion(true);
            if (f) {
                System.out.println("job任务执行成功");
            }
        }catch (Exception e){
            e.printStackTrace();
        }

    }
    static class wordMapper extends Mapper<LongWritable,Text,Text,Text>{
       public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
           String[] words = StringUtil.split(value.toString(),' ');
           for(String w:words){
               char[] c = w.toCharArray();
               Arrays.sort(c);
               String s_sort = String.valueOf(c);
               context.write(new Text(s_sort),new Text(w));
           }
       }
    }
    static class wordReducer extends Reducer<Text,Text,NullWritable,Text>{
        //定义多输出文件，根据reduce输入key值对文件命名，利用MultipleOutputs，需要加入setup和cleanup
        private MultipleOutputs<NullWritable, Text> multipleOutputs;
        public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
            for(Text t:values) {
                //多文件多名输出
                //multipleOutputs.write(NullWritable.get(), t, key.toString());
                //多目录输出
                String basePath = String.format("word=%s/part", key.toString());
                multipleOutputs.write(NullWritable.get(), t, basePath);
            }
        }
        public void setup(Context context) throws IOException, InterruptedException {
            multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
        }
        public void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
        }
    }
}
