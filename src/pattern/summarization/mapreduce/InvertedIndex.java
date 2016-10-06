/*
 * Student Info: Name=Mengchuan Lin, ID=12861
 * Subject: CS570D_HW1_Fall_2016
 * Author: Mengchuan Lin
 * Filename: InvertedIndex.java
 * Date and Time: Oct 5, 2016 10:15:00 PM
 * Project Name: mapreduce-summarization-pattern
 */
package pattern.summarization.mapreduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author Mengchuan Lin
 */
public class InvertedIndex extends Configured implements Tool {
    
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Usage: [input] [output]");
            System.exit(-1);
        }

        Job job = Job.getInstance(getConf());
        job.setJobName("wordcount");
        job.setJarByClass(InvertedIndex.class);

        /* Field separator for reducer output*/
        //job.getConfiguration().set("mapreduce.output.textoutputformat.separator", " | ");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //job.setMapperClass(WordcountMapper.class);
        //job.setCombinerClass(WordcountReducer.class);
        //job.setReducerClass(WordcountReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        Path inputFilePath = new Path(args[0]);
        Path outputFilePath = new Path(args[1]);

        /* This line is to accept input recursively */
        FileInputFormat.setInputDirRecursive(job, true);

        FileInputFormat.addInputPath(job, inputFilePath);
        FileOutputFormat.setOutputPath(job, outputFilePath);


        /* Delete output filepath if already exists */
        FileSystem fs = FileSystem.newInstance(getConf());

        if (fs.exists(outputFilePath)) {
            fs.delete(outputFilePath, true);
        }

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        InvertedIndex InvertedIndexDriver = new InvertedIndex();
        int res = ToolRunner.run(InvertedIndexDriver, args);
        System.exit(res);
    }
}
