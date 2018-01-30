import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
public class TopCategories {

    public static class Map extends Mapper<Object, Text, Text, IntWritable> {

        private Text category = new Text();
        private IntWritable one = new IntWritable(1);

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            if (line.length > 5 && line[3] != null)
                category.set(line[3]);
            // emit this value to reducer
            context.write(category, one);
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        private Text category = new Text();
        private int sum = 0;

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            for (IntWritable val : values) {
                sum = sum + val.get();
            }
            category.set(key);
            context.write(category, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "topCategory");
        job.setJarByClass(TopCategories.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
        //hadoop fs -text /Youtube/result/part-r-00000 | sort -n -k2 -r | head -n5

    }
}
