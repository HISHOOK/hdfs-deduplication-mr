package com.etc;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;

public class DedupRunner {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(DedupRunner.class);

        job.setMapperClass(DedupMapper.class);
        job.setReducerClass(DedupReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);



        FileInputFormat.setInputPaths(job,new Path("D:\\0-MINSHENG_FINTECH\\Development-documentation\\IdeaProjects\\hdfs-deduplication-mr\\mapruduces_deweighting\\input\\"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\0-MINSHENG_FINTECH\\Development-documentation\\IdeaProjects\\hdfs-deduplication-mr\\mapruduces_deweighting\\output\\"));

        job.waitForCompletion(true);
    }
}
