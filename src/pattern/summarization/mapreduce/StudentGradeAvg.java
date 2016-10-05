/*
 * Student Info: Name=Mengchuan Lin, ID=12861
 * Subject: CS570D_HW1_Fall_2016
 * Author: Mengchuan Lin
 * Filename: StudentGradeAvg.java
 * Date and Time: Oct 5, 2016 3:47:56 PM
 * Project Name: MapReduceSumPattern
 */

package pattern.summarization.mapreduce;

import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author Henry
 */


public class StudentGradeAvg {
    
    /**
     * @param args the command line arguments
     * @throws java.lang.Exception
     */
    public static void main(String[] args) throws Exception {
        java.nio.file.Path path = Paths.get("courses_output");
        if (Files.exists(path)) {
            FileUtils.deleteDirectory(path.toFile());
        }
        
        Job job = new Job();
        job.setJarByClass(StudentGradeAvg.class);
        job.setJobName("StudentGradeAvg");
        
        TextInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);
        
        //job.setMapperClass(SectionsByCourseMapper.class);
        //job.setReducerClass(SectionsByCourseReducer.class);
        
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        job.waitForCompletion(true);
        
    }
}
