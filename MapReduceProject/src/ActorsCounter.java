import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Arrays;

public class ActorsCounter extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new ActorsCounter(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "ActorsCounter");
        job.setJarByClass(this.getClass());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //DONE: set mapper and reducer class
        job.setMapperClass(ActorsCounterMapper.class);
        job.setCombinerClass(ActorsCounterCombiner.class);
        job.setReducerClass(ActorsCounterReducer.class);
        job.setMapOutputValueClass(Count.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class ActorsCounterMapper extends Mapper<LongWritable, Text, Text, Count> {

        private final Text filmId = new Text();
        private final Count counter = new Count();

        public void map(LongWritable offset, Text lineText, Context context) {
            try {
                counter.set(new IntWritable(0));
                if (offset.get() != 0) {
                    String line = lineText.toString();
                    int i = 0;
                    for (String word : line
                            .split("\\t")) {
                        if (i == 0) {
                            filmId.set(word);
                        }
                        if (i == 3) {
                            String[] actors = { "actor", "actress", "self" };
                            if (Arrays.stream(actors).anyMatch(word::contains)) {
                                counter.set(new IntWritable(1));
                            }
                        }
                        i++;
                    }
                    //DONE: write intermediate pair to the context
                    context.write(filmId, counter);

                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }


    }

    public static class ActorsCounterReducer extends Reducer<Text, Count, Text, IntWritable> {

        private final IntWritable resultValue = new IntWritable();
        Integer count;
        Count sum;

        @Override
        public void reduce(Text key, Iterable<Count> values,
                           Context context) throws IOException, InterruptedException {
            count = 0;
            sum = new Count();

            Text resultKey = new Text(key);

            for (Count val : values) {
                sum.addCount(val);
            }
            //DONE: set count variable to sum of all possible actors occurring
            count = sum.getCount().get();

            resultValue.set(count);
            //DONE: write result pair to the context
            context.write(resultKey, resultValue);

        }
    }





    public static class ActorsCounterCombiner extends Reducer<Text, Count, Text, Count> {

        private final Count counter = new Count(0);

        @Override
        public void reduce(Text key, Iterable<Count> values, Context context) throws IOException, InterruptedException {

            counter.set(new IntWritable(0));

            for (Count val : values) {
                counter.addCount(val);
            }
            context.write(key, counter);
        }
    }
}