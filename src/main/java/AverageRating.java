import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by sivagurunathanvelayutham on Jan, 2018
 */
public class AverageRating {

    public static class Map extends Mapper<Object, Text, Text, LongWritable>{
        private Text video_name = new Text();
        private LongWritable rating = new LongWritable();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            if(line[0] != null)
                video_name.set(line[0]);
            if(line[6] != null)
                rating.set(Long.parseLong(line[6]));
            context.write(video_name, rating);
        }
    }

    public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable>{
        public int count=0;
        public Long sum=0L;
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            for (LongWritable val : values) {
                sum += val.get();
                count++;
            }
            context.write(key, new LongWritable(sum/count));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration,"averageRating");

        job.setJarByClass(AverageRating.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
