package mapreduce.jiaoji;

import org.apache.derby.iapi.util.StringUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by hadoop on 2018/6/2.
 */
public class JobMain {
    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        //设置hdfs的通讯地址
        config.set("fs.defaultFS", "hdfs://219.216.64.251:9000");
        //设置RN的主机
        config.set("yarn.resourcemanager.hostname", "219.216.64.251");

        try {
            FileSystem fs = FileSystem.get(config);

            Job job = Job.getInstance(config);
            job.setJarByClass(JobMain.class);

            job.setJobName("jiaoji");
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setReducerClass(ReduceFile.class);
            //自定义输出文件名
            job.setOutputFormatClass(TestOut.class);
            TestOut.setOutputName(job, "jiaoji");
            //多文件输入
            MultipleInputs.addInputPath(job, new Path("hdfs://219.216.64.251:9000/liuxin/jiaoInput"), TextInputFormat.class,MapperFile1.class);
            MultipleInputs.addInputPath(job, new Path("hdfs://219.216.64.251:9000/liuxin/jiaoInput2"), TextInputFormat.class,MapperFile2.class);

            Path outpath = new Path("/liuxin/jiaoOutput");
            if (fs.exists(outpath)) {
                fs.delete(outpath, true);
            }
            FileOutputFormat.setOutputPath(job, outpath);

            boolean f = job.waitForCompletion(true);
            if (f) {
                System.out.println("job任务执行成功");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    static class MapperFile1 extends Mapper<LongWritable,Text,Text,Text>{
        public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
            String[] words = StringUtil.split(value.toString(),' ');
            for(String w:words){
                context.write(new Text(w),new Text("a"));
            }
        }
    }
    static class MapperFile2 extends Mapper<LongWritable,Text,Text,Text>{
        public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
            String[] words = StringUtil.split(value.toString(),' ');
            for(String w:words){
                context.write(new Text(w),new Text("b"));
            }
        }
    }
    static class ReduceFile extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
            int i = 0;
            int j = 0;
            for(Text w:values){
                if(w.equals(new Text("a"))) {
                    i++;
                }else{
                    j++;
                }
            }
            if(i>0 && j>0){
                context.write(key,new Text("true"));
            }
        }
    }
    //自定义输出文件名函数
    private static class TestOut extends TextOutputFormat {
        protected static void setOutputName(JobContext job, String name) {
            job.getConfiguration().set(BASE_OUTPUT_NAME, name);
        }
    }

}
