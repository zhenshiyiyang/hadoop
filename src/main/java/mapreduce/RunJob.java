package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by hadoop on 2018/4/22.
 */
public class RunJob {
    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        //设置hdfs的通讯地址
        config.set("fs.defaultFS", "hdfs://219.216.64.251:9000");
        //设置RN的主机
        config.set("yarn.resourcemanager.hostname", "219.216.64.251");

        try {
            FileSystem fs = FileSystem.get(config);

            Job job = Job.getInstance(config);
            job.setJarByClass(RunJob.class);

            job.setJobName("wc");

            job.setMapperClass(WcMapper.class);
            job.setReducerClass(WcReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job, new Path("/liuxin/input"));

            Path outpath = new Path("/liuxin/output/wc");
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
}