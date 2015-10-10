import java.io.IOException;
import java.util.Iterator;
import java.util.HashMap;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;

public class SimilarNetflixUsersReducer extends MapReduceBase implements Reducer<Text, Text, Text, IntWritable> {

  private final IntWritable one = new IntWritable(1);
  //private static String user;

  @Override
  public void reduce(Text key, Iterator values, OutputCollector output, Reporter reporter) throws IOException {

    String user = "822109";
    String rating = "";
    //Map for caching users as we iterate to find the user
    HashMap<String, ArrayList<String>> usersPerRating = new HashMap<>();
    //Initalize the map to have the 5 ratings already
    usersPerRating.put("1", new ArrayList<String>());
    usersPerRating.put("2", new ArrayList<String>());
    usersPerRating.put("3", new ArrayList<String>());
    usersPerRating.put("4", new ArrayList<String>());
    usersPerRating.put("5", new ArrayList<String>());

    //Key = Movie, value = "user rating"

    //Find the user and what they rated the movie, cache others
    while(values.hasNext()) {
      //Parse "user rating"
      Text userAndRatingText = (Text) values.next();
      String userAndRating = userAndRatingText.toString();
      String[] parsed = userAndRating.split("[ \\.\\?,\\n\\r\\t]+");
      //Check if we found user
      if(parsed[0].equals(user)) {
        //Found user set the rating to what they rated
        rating = parsed[1];
        //break out
        break;
      }
      //If not the user add to a cache for checking again later
      else {
        //use rating as key to get array to add user to
        usersPerRating.get(parsed[1]).add(parsed[0]);
      }
    }
    //System.out.println("Movie: " + key.toString() + "\n");
    //System.out.println("Rating: " + rating + "\n");
    //System.out.println("Rating 1 size = " + Integer.toString(usersPerRating.get("1").size()) + "\n");
    //System.out.println("Rating 2 size = " + Integer.toString(usersPerRating.get("2").size()) + "\n");
    //System.out.println("Rating 3 size = " + Integer.toString(usersPerRating.get("3").size()) + "\n");
    //System.out.println("Rating 4 size = " + Integer.toString(usersPerRating.get("4").size()) + "\n");
    //System.out.println("Rating 5 size = " + Integer.toString(usersPerRating.get("5").size()) + "\n");
    //If user didn't rate this movie then just output none for sanity check and quit
    if(rating.equals("")) {
      //output.collect(new Text("Movie: " + key.toString() + " None"), one);
      return;
    }

    //System.out.println("ACTUALLY CHECKING USERS NOW\n");

    //Add cached users first
    ArrayList<String> usersWithSameRating = usersPerRating.get(rating);
    if(usersWithSameRating.size() > 0) {
      for(String userID : usersWithSameRating) {
        output.collect(new Text(userID), one);
      }
    }

    //Find users who have rated the movie the same
    while(values.hasNext()) {
      //Parse "user rating"
      Text userAndRatingText = (Text) values.next();
      String userAndRating = userAndRatingText.toString();
      String[] parsed = userAndRating.split("[ \\.\\?,\\n\\r\\t]+");
      //check rating against the users rating
      if(parsed[1].equals(rating)) {
        output.collect(new Text(parsed[0]), one);
      }
    }

 
  }

  /*public void configure(JobConf job) {
    user = job.get("user");
  }*/
}
