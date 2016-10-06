/*
 * Student Info: Name=Mengchuan Lin, ID=12861
 * Subject: CS570D_HW1_Fall_2016
 * Author: Mengchuan Lin
 * Filename: InvertedIdx.java
 * Date and Time: Oct 5, 2016 10:15:00 PM
 * Project Name: mapreduce-summarization-pattern
 */
package pattern.summarization.mapreduce;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author Mengchuan Lin
 */
public class InvertedIdx extends Configured implements Tool {
    
    public static class InvertedIdxMapper extends Mapper<Object, Text, Text, Text> {

        private final Text word = new Text();
        private Text fileName = new Text();

        private boolean caseSensitive;

        @Override
        public void map(Object key, Text value, Mapper.Context context)
                throws IOException, InterruptedException {
            //obtain the file name of the input file which contains the key word
            String fileNameStr = ((FileSplit) context.getInputSplit()).getPath().getName();
            StringBuilder sbr = new StringBuilder();
            sbr = sbr.append("{").append(fileNameStr).append(":");
            fileName = new Text(sbr.toString());

            String sentence = value.toString();
            String words[];

            if (!caseSensitive) {
                sentence = sentence.toLowerCase();
            }

            /*split the sentence into words separated by blank space and save
            each word into the words array*/
            words = sentence.split(" ");
            for (String i : words) {
                word.set(i);
                context.write(word, fileName);
            }
        }
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            this.caseSensitive = conf.getBoolean("InvertedIdx.case.sensitive", true);
        }
    }
    
    public static class InvertedIdxReducer extends Reducer<Text, Text, Text, Text> {
        
        private final Text postingsList = new Text();
        
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            StringBuilder sbr = new StringBuilder();
            String prevStr = "";
            Integer wordCount = 0;
            
            /*iterate through list of file names that correspond to key words
            and count the number of occurrences of each word in separate files*/
            Iterator<Text> iter = values.iterator();
            while (iter.hasNext()) {
                String currStr = iter.next().toString();
                //compare current fileName string with previous string
                if (!currStr.equals(prevStr)) {
                    //if fileName is the first one encountered, append name only
                    if (sbr.toString().isEmpty())
                        sbr = sbr.append(currStr);
                    /*if a new file name is read, append name and count, then
                    reset count*/
                    else {
                        sbr.append(wordCount.toString()).append("}").append(",").append(currStr);
                        wordCount = 0;
                    }
                }
                ++wordCount;
                prevStr = currStr;
                /*if last element is encountered in the fileName values list,
                append final word count and closing brace*/
                if (!iter.hasNext())
                    sbr.append(wordCount.toString()).append("}");
            }
            
            postingsList.set(sbr.toString());
            context.write(key, postingsList);
        }
    }
    
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Usage: [input] [output]");
            System.exit(-1);
        }

        Job job = Job.getInstance(getConf());
        job.setJobName("InvertedIdx");
        job.setJarByClass(InvertedIdx.class);

        /* Field separator for reducer output*/
        //job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ";");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(InvertedIdxMapper.class);
        job.setReducerClass(InvertedIdxReducer.class);

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
        InvertedIdx InvertedIdxDriver = new InvertedIdx();
        int res = ToolRunner.run(InvertedIdxDriver, args);
        System.exit(res);
    }
}
