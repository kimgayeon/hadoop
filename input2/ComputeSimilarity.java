

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Arrays;

public class ComputeSimilarity {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
          
        /* create a job and specify the jobName "ComputeSimilarity" */
        Job job = Job.getInstance(conf, "ComputeSimilarity");
        //FileInputFormat.setInputDirRecursive(job, true);
        /* pass the data url */
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        /* because it finds uniquemovie, the number of reducerTasks should be 1 */
        job.setNumReduceTasks(1);

        /* set the jar by finding where a given class came from */
        job.setJarByClass(ComputeSimilarity.class);

        /* each code sets the Mapper, Combiner, reducer for the job. */
        job.setMapperClass(Map.class);
        //job.setCombinerClass(Combine.class);
        job.setReducerClass(Reduce.class);

        /* Set the key and value class for the map output data.
        we can see it is same with Mapper class' output format */
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        /* Set the key and value class for the job output */
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        // Submit the job and wait for it's completion
        job.waitForCompletion(true);
    }

    /* input : text
    <단어, [(리뷰어, 값), (리뷰어, 값) ... ]> */



public static class Pair implements Comparable<Pair>
{
    private String reviewerId;
    private double tfidfValue;

    public Pair(String reviewer, double tfidf)
    {
        this.reviewerId = reviewer;
        this.tfidfValue = tfidf;
    }

    public String getReviewerId()
    {
        return this.reviewerId;
    }

    public double getTfidfValue()
    {
        return this.tfidfValue;
    }

    public String toString()
    {
        return reviewerId;
    }

    @Override 
    public int compareTo(Pair o) {
        Pair p = (Pair) o; 
       	return this.getReviewerId().compareTo(p.getReviewerId());
    }
}

    public static class Map extends Mapper<Text, Text, Text, DoubleWritable> {
        //private Text titles = new Text();

        public void map(Text key, Text value, Context context) 
                throws IOException, InterruptedException {
            /* 
               받아온 키 : 단어
               받아온 벨류 : 리뷰어, tf-idf값 
            */

            // String morph = key.toString();
            // String docsAndTfidf = value.toString();
            
            String docsAndTfidf = value.toString();

            String[] docAndTfidf = docsAndTfidf.split(",");
            //docAndTfidf에는  리뷰어:tf-idf 가 각각 들어가 있는 상태이다.


            String[] splitedValue;

            ArrayList<Pair> reviewerList = new ArrayList<>();

            for(String tmpDocAndTfidf : docAndTfidf)
            {
                splitedValue = tmpDocAndTfidf.split(":");
                Pair p = new Pair(splitedValue[0], Double.parseDouble(splitedValue[1]));
                reviewerList.add(p);
            }    

            Collections.sort(reviewerList);
                
            int listSize = reviewerList.size();
            
            //pair쌍 구하기
            for(int i = 0; i < listSize-1; i++)
            {
                for(int j = i+1 ; j < listSize; j++)
                {
                    String pairReviewer = ((reviewerList.get(i)).getReviewerId()) + " " + ((reviewerList.get(j)).getReviewerId());
                    double pairTfIdf = ((reviewerList.get(i)).getTfidfValue()) * ((reviewerList.get(j)).getTfidfValue());
                    context.write(new Text(pairReviewer), new DoubleWritable(pairTfIdf));
                }
            }

        }
    }



    public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            
            double similarity = 0;
            for(DoubleWritable value : values){
               similarity += value.get();

            }
             context.write(key, new DoubleWritable(similarity));
        }
        /* Called once at the end of the task. 
        call cleanup method to write the number of unique movie titles*/       
    }
}
