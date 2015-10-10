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

public class SimilarNetflixUsersMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {


  @Override
  public void map(LongWritable key, Text value, OutputCollector<Text, Text> output,
                  Reporter reporter) throws IOException {
    //User movie rank
    //String user = "2";
    //Get String Representation of line
    String line = value.toString();
    //Parse line into movie and user\srating combos the file is user \s movie \s rating all numerical
    String[] parsed = line.split("[ \\.\\?,\\n\\r\\t]+");
    
    //key = movie
    String key_r = parsed[1];
    //value = user rating
    String value_r = parsed[0] + " " + parsed[2];
    //Out (movie, user rating)
    output.collect(new Text(key_r), new Text(value_r));

  }

}