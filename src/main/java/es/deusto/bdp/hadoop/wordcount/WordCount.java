package es.deusto.bdp.hadoop.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.NullWritable;

public class WordCount {
    public static class WordCountMapper
       extends Mapper<Object, Text, Text, IntWritable> {

        IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context
                   ) throws IOException, InterruptedException {
            
            String[] words = value.toString().split("\\W+");
            for(String w : words) {
                context.write(new Text(w), one);
            }
        }
    }

    public static class WordCountReducer
       extends Reducer<?, ?, ?, ?> {

        public void reduce(? key, Iterable<?> values,
                       Context context
                       ) throws IOException, InterruptedException {

            // Insert your code here   

        }
    }

     public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Union");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);

        job.setOutputKeyClass(?.class);
        job.setOutputValueClass(?.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
