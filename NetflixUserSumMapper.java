import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class NetflixUserSumMapper extends MapReduceBase implements Mapper<Text, Text, Text, IntWritable> {

  private final IntWritable one = new IntWritable(1);
  
  @Override
  public void map(Text key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

    output.collect(key, one);

  }
}