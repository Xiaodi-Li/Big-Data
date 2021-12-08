
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Collections;
import java.util.Comparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.List;

public class InMemoryJoinReducerCntAge {

	public static class FriendMapper extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			String[] item = line.split("\\t");

			String user = item[0];
			String others = item.length > 1 ? item[1] : "";

			context.write(new Text(user), new Text(others));
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		private HashMap<String, String> userAddress;
		private HashMap<String, String> userBirth;
		private HashMap<String, Double> ages = new HashMap<String, Double>();

		@Override
		public void setup(Context context) throws IOException, InterruptedException {

			userAddress = new HashMap<String, String>();
			userBirth = new HashMap<String, String>();
			String userFile = context.getConfiguration().get("userdata");
			Path path = new Path(userFile);// Location of file in HDFS
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
			String line = br.readLine();
			while (line != null) {
				String[] userDetail = line.split(",");
				String temp = userDetail[3] + "," + userDetail[4] + "," + userDetail[5]; // Dealing with user's address

				userAddress.put(userDetail[0], temp);

				String birth = userDetail[9]; // Dealing with user's ages
				DateFormat format = new SimpleDateFormat("MM/dd/yyyy");
				Date birthDay;
				try {
					birthDay = format.parse(birth);
					Calendar dob = Calendar.getInstance();
					dob.setTime(birthDay);
					Calendar today = Calendar.getInstance();
					int age = today.get(Calendar.YEAR) - dob.get(Calendar.YEAR);
					String outage = age + "";

					userBirth.put(userDetail[0], outage);

				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				line = br.readLine();
			}
			br.close();
		}

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			List<String> friAge = new ArrayList<String>();
			String user = key.toString();
			for (Text val : values) {
				String[] friends = val.toString().split(",");
				for(String friend : friends) {
					//System.out.println(friend);
					if (userBirth.containsKey(friend)){
						friAge.add(userBirth.get(friend));	
					}
						
				}
			}

//			String output = "";
			double avgAge = 0.0;
//			for (String fri : friAge) {
			if (friAge.size() > 0 && friAge.toString()!=null) {
	            //System.out.println("friAge = "+friAge.toString());
				double res = 0.0;
				for (int i = 0; i < friAge.size(); i++) {
					res += Double.parseDouble(friAge.get(i));
				}
				avgAge = res / friAge.size();
			}
//			}
			String outKey;
			
            outKey = user + "," + (String)this.userAddress.get(user);
            //this.ages.put(outKey, avgAge);
            this.ages.put(user, avgAge);

			// String outKey = user+","+userAddress.get(user);
			//context.write(key, new Text(String.valueOf(avgAge)));
		}
		
        protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            List<Entry<String, Double>> resList = new ArrayList(this.ages.entrySet());
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
                String outAge = "" + entry.getValue();
                context.write(new Text(outkey), new Text(outAge));
            }

        }
	}

// Driver program
	public static void main(String[] args) throws Exception {
		// Standard Job setup procedure.
		Configuration conf = new Configuration();
		FileSystem FS = FileSystem.get(conf);
		FS.delete(new Path(args[1]), true);
		String userdata = args[2];
		conf.set("userdata", userdata);

		Job job = new Job(conf, "InMemoryJoinReducerCntAge");
		job.setJarByClass(InMemoryJoinReducerCntAge.class);
		job.setMapperClass(FriendMapper.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}