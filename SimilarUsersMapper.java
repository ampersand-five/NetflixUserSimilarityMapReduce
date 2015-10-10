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

public class SimilarUsersMapper extends MapReduceBase implements Mapper<Text, Text, IntWritable, Text> {

  private final IntWritable one = new IntWritable(1);

  @Override
  public void map(Text key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {

    String userSum = key.toString() + " " + value.toString();
    output.collect(one, new Text(userSum));


  }
}