//
//by www.olo.how
//Code for hadoop. Count fruit and sort
//


import java.io.FileWriter;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FruitCountOrder {
  
	//map part
	public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text fruit = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());

      while (itr.hasMoreTokens()) {
        fruit.set(itr.nextToken());
        context.write(fruit, one);
      }
    }
  }

  //reduce part
  public static class SumFruitReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();    
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {

      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }

      result.set(sum);
      context.write(key, result);
    }
  }

  //sort by value
  public static class SortFruit extends WritableComparator{

      protected SortFruit() {
          super(Text.class, true);
          }

      @SuppressWarnings({ "rawtypes" })
	  @Override
      public int compare(WritableComparable w1,WritableComparable w2){

          Text f1 = (Text) w1;
          Text f2 = (Text) w2;
          return f1.compareTo(f2);
      }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Fruit count and sort");
    job.setJarByClass(FruitCountOrder.class);
    job.setSortComparatorClass(SortFruit.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(SumFruitReducer.class);
    job.setReducerClass(SumFruitReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}