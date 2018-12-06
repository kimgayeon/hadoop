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
            
            desc.replace("*�ٸ� ���е���  ~~~       ������)","");
            desc.replace("* ��ǰ�ı�� 5��  ~~~       ������)","");
            desc.replace("*�Ʒ��� �ʼ������� �ݵ�� �������ּ���^^","");
            desc.replace("*��, �ݺ��Ǵ� �ܾ ������ ����ϽǼ� �����ϴ�.","");
            desc.replace("- Ű, ������  ~~~ ������)","");
            desc.replace("- Ű, ������  ~~~  �����Ĵ�����?","");
            desc.replace("Ű, ������ ~~~ ������)","");
            desc.replace("Ű, ������ ~~~ �����Ĵ�����?","");
            desc.replace("*��ǰ�ı�� ~~~ ���ּž��մϴ�.","");
            desc.replace("Ű, ������ ~~~ �����Ĵ�����:","");
            desc.replace("�����ı� [0-5]��","");
            
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