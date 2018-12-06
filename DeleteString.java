import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class DeleteString {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        /* Create a new job*/
        Job job = Job.getInstance(conf, "deletestring");

        /* Use the WordCount.class file to point to the job jar*/
        job.setJarByClass(DeleteString.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        /* Setting the input and output locations*/
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        /* Submit the job and wait for it's completion*/
        job.waitForCompletion(true);
    }

    public static class Map extends Mapper<Text, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("|");
            String cId = fields[0];
            String desc = fields[1];
            
            desc.replace("*다른 고객분들이  ~~~       나쁜점)","");
            desc.replace("* 상품후기는 5줄  ~~~       나쁜점)","");
            desc.replace("*아래의 필수사항은 반드시 기재해주세요^^","");
            desc.replace("*단, 반복되는 단어나 문장은 사용하실수 없습니다.","");
            desc.replace("- 키, 몸무게  ~~~ 나쁜점)","");
            desc.replace("- 키, 몸무게  ~~~  착용후느낌은?","");
            desc.replace("키, 몸무게 ~~~ 나쁜점)","");
            desc.replace("키, 몸무개 ~~~ 착용후느낌은?","");
            desc.replace("*상품후기는 ~~~ 써주셔야합니다.","");
            desc.replace("키, 몸무게 ~~~ 착용후느낌은:","");
            desc.replace("포토후기 [0-5]등","");
            
            word.set(cId);
            context.write(word, desc);
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            
            /* Sum all the occurrences of the word (key) */
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
}