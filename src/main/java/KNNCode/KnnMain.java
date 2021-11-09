package KNNCode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

public class KnnMain {


    public static void main(String[] args) throws Exception {
        //String inputFile = "E:\\IdeaProjects\\homework07\\input\\train";
        String inputFile = args[0]+"train";
        //任务一
        Configuration conf = new Configuration();
        String[] otherArgs = new String[]{
                args[0]+"train",
                args[1]
        };
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        System.out.println(args[0]);
        conf.setInt("K", 5);
        Job job1 = Job.getInstance(conf, "word count");
        job1.setJarByClass(WordCount.class);
        //设置分布式缓存文件
        //job1.addCacheFile(new URI("hdfs://localhost:9000/input/iris/test/iris_test_data.csv"))
        //job1.addCacheFile(new URI("file:///E:/IdeaProjects/homework07/input/test"));
        job1.addCacheFile(new URI(args[0]+"test"));
        job1.setMapperClass(TokenizerMapper.class);
        job1.setCombinerClass(InvertedIndexCombiner.class);
        job1.setReducerClass(IntSumReducer.class);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);
        //Path outputFile = new Path("E:\\IdeaProjects\\homework07\\output");
        Path outputFile=new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputFile)) {
            fs.delete(outputFile, true);
        }
        FileInputFormat.setInputPaths(job1, inputFile);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job1, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job1,
                new Path(otherArgs[otherArgs.length - 1]));

        job1.waitForCompletion(true);

    }


}




