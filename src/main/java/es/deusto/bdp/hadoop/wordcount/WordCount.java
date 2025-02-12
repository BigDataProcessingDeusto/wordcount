package es.deusto.bdp.hadoop.wordcount;

import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
    public static class WordCountMapper
       extends Mapper<Object, Text, Text, IntWritable> {

        IntWritable one = new IntWritable(1);
        Pattern pattern = Pattern.compile("\\w+");

        public void map(Object key, Text value, Context context
                   ) throws IOException, InterruptedException {
            
            Matcher matcher = pattern.matcher(value.toString().toLowerCase());
            while(matcher.find()) {
                context.write(new Text(matcher.group(0)), new IntWritable(1));
            }
            // String[] words = value.toString().split("\\w+");
        //     for(String w : words) {
        //         context.write(new Text(w), one);
        //     }
        }
    }

    public static class WordCountReducer
       extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {

            int result = 0;
            for(IntWritable v : values) {
                result += v.get();
            }  
            
            context.write(key, new IntWritable(result));
        }
    }

     public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Union");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("input"));
        FileOutputFormat.setOutputPath(job, new Path("output"));
        // FileInputFormat.addInputPath(job, new Path(args[0]));
        // FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
