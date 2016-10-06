/*
 * Student Info: Name=Mengchuan Lin, ID=12861
 * Subject: CS570D_HW1_Fall_2016
 * Author: Mengchuan Lin
 * Filename: StudentGradeAvg.java
* Date and Time: Oct 5, 2016 3:47:56 PM
 * Project Name: MapReduceSumPattern
 */

package pattern.summarization.mapreduce;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author Henry
 */


public class StudentGradeAvg {
        
    public static class GradeAvgMapper extends Mapper<Object, Text, Text, Text> {
        private final Text stuName = new Text();
        private final Text courseName = new Text();
        
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String tokens[] = value.toString().split(",");
            
            for (int i = 1; i < tokens.length; ++i) {
                /*split CourseName(Marks) string into CourseName and Marks
                using regex*/
                String nameAndMarks[] = tokens[i].split("[()]+");
                //set key to be name of student
                stuName.set(nameAndMarks[0]);
                //append marks obtained by student to course
                StringBuilder sbr = new StringBuilder();
                sbr = sbr.append(tokens[0]).append("(").append(nameAndMarks[1]).append(")");
                courseName.set(sbr.toString());
                context.write(stuName, courseName);
            }
        }
    }
    
    public static class GradeAvgReducer extends Reducer<Text, Text, Text, Text> {
        private final Text stuAvgMarks = new Text();
        private final Text courseMarks = new Text();
        
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            ArrayList<String> courseMarksList = new ArrayList<>();
            String marks;
            String tokens[];
            StringBuilder sbr = new StringBuilder();
            
            Float avgMarks;
            float mark;
            float totalMarks = 0f;
            int count = 0;
            
                for (Text value : values) {
                    marks = value.toString();
                    courseMarksList.add(marks);
                    /*split and extract digits enclosed by parenthesis from
                    string*/
                    tokens = marks.split("[()]+");
                    mark = Float.parseFloat(tokens[1]);
                    //sum up total marks obtained for each course by student
                    totalMarks += mark;
                    ++count;
                }
                //calculate average marks
                avgMarks = totalMarks / count;
                //append average marks enclosed in parenthesis to student's name
                sbr = sbr.append(key).append("(").append(avgMarks.toString()).append(")");
                stuAvgMarks.set(sbr.toString());
                sbr.delete(0, sbr.length());
                /*build comma-separated string consisting of a list of courses
                that a student has taken*/
                for (Iterator<String> iter = courseMarksList.iterator(); iter.hasNext();) {
                    sbr = sbr.append(iter.next());
                    //append comma only if not at last element of ArrayList
                    if (iter.hasNext())
                        sbr.append(",");
                }

                courseMarks.set(sbr.toString());
                context.write(stuAvgMarks, courseMarks);                
        }
    }
    
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
        
        job.setMapperClass(GradeAvgMapper.class);
        job.setReducerClass(GradeAvgReducer.class);
        
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.waitForCompletion(true);        
    }
}
