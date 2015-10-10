import java.io.IOException;
import java.util.Iterator;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class SimilarUsersReducer extends MapReduceBase
    implements Reducer<IntWritable, Text, IntWritable, Text> {

  @Override
  public void reduce(IntWritable key, Iterator values, OutputCollector output, Reporter reporter) throws IOException {

    int greatest = 0;
    ArrayList<String> users = new ArrayList<String>();

    while(values.hasNext()) {
      //Parse "user sum"
      Text userSumText = (Text) values.next();
      String userSum = userSumText.toString();
      String[] parsed = userSum.split("[ \\.\\?,\\n\\r\\t]+");
      //Check against max
      int currentSum = Integer.parseInt(parsed[1]);
      //If greater, update greatest, clear array, store current user
      if(currentSum > greatest) {
        greatest = currentSum;
        users.clear();
        users.add(parsed[0]);
      }
      //If the same, add the user
      else if(currentSum == greatest) {
        users.add(parsed[0]);
      }
      //else continue to next
      //If not the user add to a cache for checking again later
    }

    //Iterate through users array and print out any in it, should have at least one
    Iterator iter = users.iterator();

    while (iter.hasNext()) {
      output.collect(new IntWritable(greatest), new Text((String)iter.next()));
    }

  }
}