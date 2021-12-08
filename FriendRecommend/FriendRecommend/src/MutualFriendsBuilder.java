import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.util.List;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashSet;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.io.IOException;
import java.lang.Integer;


public class MutualFriendsBuilder {
    
    public MutualFriendsBuilder() {
    }
    
    
	public static class FriendMapper extends Mapper<LongWritable, Text, Text, Text> {
        Text userID = new Text();
        Text friends = new Text();

        public FriendMapper() {
        }        

        public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String []item = line.split("\\t");
			
			String user = item[0];
			String others = item.length>1?item[1]:"";
			context.write(new Text(user), new Text(others));			

        }
    }

    
	public static class FriendReducer extends Reducer<Text, Text, Text, Text> {
		
        Text listResult = new Text();

        public FriendReducer() {
        }
        
        private HashMap<String, String> user_friends_hash = new HashMap<>();
        
        public void setup(Context context) throws IOException, InterruptedException{
         String userFile = context.getConfiguration().get("ARGUMENT");
               Path path=new Path(userFile);//Location of file in HDFS
               FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(path)));
               String line=br.readLine();
               while (line != null){
          String[] userAndFriends = line.split("\t");
          if (userAndFriends.length == 2) {
           String user = userAndFriends[0];
           String friends = userAndFriends[1].toString();
           user_friends_hash.put(user, friends);
          }
                   line=br.readLine();
               }
               br.close();
           }

        private String intersection(String[] s1, String[] s2) {
            HashSet<String> h = new HashSet<String>();
            StringBuilder sb = new StringBuilder();

            int j;
            for(j = 0; j < s1.length; ++j) {
                h.add(s1[j]);
            }

            for(j = 0; j < s2.length; ++j) {
                if (h.contains(s2[j])) {
                    sb.append(s2[j] + " ");
                }
            }

            return sb.toString();
        }

		public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			Text result = new Text();
			//System.out.println("size of user_friend_hash=" + user_friends_hash.size());
			Iterator<Text> valueItem = values.iterator();
			while (valueItem.hasNext()) {
				Iterator<Entry<String, String>> hash_iter = user_friends_hash.entrySet().iterator();
				String[] friendList2 = valueItem.next().toString().split(",");
				int counter = 0;
				while (hash_iter.hasNext()) {
					Entry<String, String> next_item = hash_iter.next();
					String key2 = next_item.getKey();
					//System.out.println("key2 in line 138=" + key2);
					String[] friendList1 = next_item.getValue().split(",");
					if (!key.toString().equals(key2) && Integer.valueOf(key2) > Integer.valueOf(key.toString())) {
						//System.out.println("key in line 143=" + key.toString());
						String common_friends = this.intersection(friendList1, friendList2);
						if (common_friends.strip() != "") {
							Text friend_pair = new Text();
							if (Integer.valueOf(key2) > Integer.valueOf(key.toString())) {
								friend_pair.set(key.toString() + "," + key2);
							} else {
								friend_pair.set(key2 + "," + key.toString());
							}
							result.set(common_friends);
							context.write(friend_pair, result);
						}
						//System.out.println("counter of values from reducer=" + counter);
						counter += 1;
					}
				}
				System.out.println("counter for each key="+counter+" key="+key.toString());
			}
	    }
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		//String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    	
		FileSystem FS = FileSystem.get(conf);
    	FS.delete(new Path(args[1]),true);
    	String userInput = args[2];
		conf.set("ARGUMENT",userInput);
		

		Job job = new Job(conf, "Friend_Mutation");
		job.setJarByClass(MutualFriendsMaxcounts.class);
		job.setMapperClass(FriendMapper.class);
		job.setReducerClass(FriendReducer.class);

		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(Text.class);
		// set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(args[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	

}
