import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;

public class SimilarNetflixUsers {

  public static void main(String[] args) {
    JobClient client = new JobClient();
    JobConf conf = new JobConf(SimilarNetflixUsers.class);

    // specify output types
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);
    conf.setMapOutputValueClass(Text.class);

    //conf.set("userId", args[4]);

    // specify input and output dirs
    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    // specify a mapper
    conf.setMapperClass(SimilarNetflixUsersMapper.class);

    // specify a combiner. For this one we can use the reducer code
   // conf.setCombinerClass(WordCountReducer.class);

    // specify a reducer
    conf.setReducerClass(SimilarNetflixUsersReducer.class);

    conf.setNumReduceTasks(128);
    
    client.setConf(conf);
    try {
      JobClient.runJob(conf);
    } catch (Exception e) {
      e.printStackTrace();
    }


    //Second mapper, setting addInputPath() with a directory uses all files in the directory as input
    JobClient client2 = new JobClient();
    JobConf conf2 = new JobConf(SimilarNetflixUsers.class);
    // specify output types
    conf2.setOutputKeyClass(Text.class);
    conf2.setOutputValueClass(IntWritable.class);
    //conf.setMapOutputValueClass(Text.class);

    // specify input and output dirs
    FileInputFormat.addInputPath(conf2, new Path(args[1]));
    FileOutputFormat.setOutputPath(conf2, new Path(args[2]));

    // specify a mapper
    conf2.setMapperClass(NetflixUserSumMapper.class);

    // specify a combiner. For this one we can use the reducer code
   // conf.setCombinerClass(WordCountReducer.class);

    // specify a reducer
    conf2.setReducerClass(NetflixUserSumReducer.class);

    conf2.setNumReduceTasks(128);

    conf2.setInputFormat(KeyValueTextInputFormat.class);
    
    client2.setConf(conf2);
    try {
      JobClient.runJob(conf2);
    } catch (Exception e) {
      e.printStackTrace();
    }


//		args[0] 1stIn		args[1] 1stOut 2ndIn	args[2] 2ndOut 3rdIn	args[3] 3rdOut		args[4] userID	
    //Third mapper
    JobClient client3 = new JobClient();
    JobConf conf3 = new JobConf(SimilarNetflixUsers.class);
    // specify output types
    conf3.setOutputKeyClass(IntWritable.class);
    conf3.setOutputValueClass(Text.class);
    //conf.setMapOutputValueClass(Text.class);

    // specify input and output dirs
    FileInputFormat.addInputPath(conf3, new Path(args[2]));
    FileOutputFormat.setOutputPath(conf3, new Path(args[3]));

    // specify a mapper
    conf3.setMapperClass(SimilarUsersMapper.class);

    // specify a combiner. For this one we can use the reducer code
   // conf.setCombinerClass(WordCountReducer.class);

    // specify a reducer
    conf3.setReducerClass(SimilarUsersReducer.class);

    conf3.setNumReduceTasks(1);

    conf3.setInputFormat(KeyValueTextInputFormat.class);
    
    client3.setConf(conf3);
    try {
      JobClient.runJob(conf3);
    } catch (Exception e) {
      e.printStackTrace();
    }



  }
}
