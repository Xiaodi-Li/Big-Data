import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map.Entry;
import java.lang.Double;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.hadoop.io.IntWritable;
import java.util.*;
import org.apache.hadoop.util.StringUtils;

public class MutualFriendsMaxcounts {
	public static class FriendMapper extends Mapper<LongWritable, Text, Text, Text> {
		
        public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String []item = line.split("\\t");
			
			String user = item[0];
			String others = item.length>1?item[1]:"";
			context.write(new Text(user), new Text(others));			

        }
	}
	
	public static class FriendCombiner extends Reducer<Text, Text, Text, Text> {
        Text listResult = new Text();

        public FriendCombiner() {
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
        
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
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
							//context.write(new LongWritable(common_friends.split(",").length), friend_pair);
							context.write(friend_pair, new Text(String.valueOf(common_friends.split("\\s+").length)));
						}
						//System.out.println("counter of values from reducer=" + counter);
						counter += 1;
					}
				}
				System.out.println("counter for each key="+counter+" key="+key.toString());
			}
	    }
	}
	
	public static class FriendReducer extends Reducer<Text, Text, Text, Text> {
        // variable to keep track of number of records emitted
        private static int count = 0;
        private HashMap<String, Double> friendPairLength = new HashMap<String, Double>();
//        private Text keyOutput = new Text();
//        private Text valueOutput = new Text();
        
        /**
         * As there is only 1 Reducer, it gets all the value emitted by Mapper in descending order
         * of the number of the mutual friends for a pair of users. And it just write top 10 records
         * to the output file.
         * @param key key emitted by mapper
         * @param values list of value emitted by mapper for the key
         * @param context context object to interact with rest of hadoop system
         * @throws IOException
         * @throws InterruptedException
         */
        
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
            for (Text val : values) {
                //context.write(key, valueOutput);
            	this.friendPairLength.put(key.toString(), Double.valueOf(val.toString()));
                count++;
            }
		}
		
		
        protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            List<Entry<String, Double>> resList = new ArrayList(this.friendPairLength.entrySet());
            Collections.sort(resList, new Comparator<Entry<String, Double>>() {
                public int compare(Entry<String, Double> e1, Entry<String, Double> e2) {
                    return ((Double)e2.getValue()).compareTo((Double)e1.getValue());
                }
            });
            int count = 0;

            for(Iterator var5 = resList.iterator(); var5.hasNext(); ++count) {
                Entry<String, Double> entry = (Entry)var5.next();
                if (count >= 400) {
                    break;
                }

                String outkey = (String)entry.getKey();
                String friends_length = "" + entry.getValue();
                context.write(new Text(outkey), new Text(friends_length));
            }

        }
	    
	}
	
//    public static class FriendReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
//    	private Text valueOutput = new Text();
//    	HashMap<String, Integer> hash = new HashMap();
//        public void reduce(IntWritable key, Iterable<Text> values, Context context) 
//        		throws IOException, InterruptedException {
//            // Convert hash to list and remove paths of length 1.
//        	for (Text val : values) {
//        		
//        	}
//            ArrayList<Entry<String, Integer>> list = new ArrayList<Entry<String, Integer>>();
//            for (Entry<String, Integer> entry : hash.entrySet()) {
//                if (entry.getValue() != -1) {   // Exclude paths of length 1.
//                    list.add(entry);
//                }
//            }
//            // Sort key-value pairs in the list by values (number of common friends).
//            Collections.sort(list, new Comparator<Entry<String, Integer>>() {
//                public int compare(Entry<String, Integer> e1, Entry<String, Integer> e2) {
//                    return e2.getValue().compareTo(e1.getValue());
//                }
//            });
//            int MAX_RECOMMENDATION_COUNT = 10;
//            if (MAX_RECOMMENDATION_COUNT < 1) {
//                // Output all key-value pairs in the list.
//                context.write(key, new Text(StringUtils.join(",", list)));
//            } 
//            else {
//                // Output at most MAX_RECOMMENDATION_COUNT keys with the highest values (number of common friends).
//                ArrayList<String> top = new ArrayList<String>();
//                for (int i = 0; i < Math.min(MAX_RECOMMENDATION_COUNT, list.size()); i++) {
//                    top.add(list.get(i).getKey());
//                }
////                for(int i=0;i<inputItem.length;i++){
////                	 if(key.toString().equals(inputItem[i])){
////                		 context.write(key, new Text(StringUtils.join(",", top)));
////                	 }
////                }
//               
//            }
//        }
//    }

	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		//String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		FileSystem FS = FileSystem.get(conf);
    	FS.delete(new Path(args[1]),true);
		String userInput = args[2];
		conf.set("ARGUMENT",userInput);
		
		Job job = new Job(conf, "MutualFriendsMaxcounts");
		job.setJarByClass(MutualFriendsMaxcounts.class);
		job.setMapperClass(FriendMapper.class);
		job.setCombinerClass(FriendCombiner.class);
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
