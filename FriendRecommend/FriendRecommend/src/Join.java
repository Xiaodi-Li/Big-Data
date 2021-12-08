
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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

public class Join{
	

	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {			
			//readline doc
			String line = value.toString();
			String []two_friends_list = line.split("\\t");
			context.write(new Text(two_friends_list[0]), new Text(two_friends_list[1]));
		}	
		
	}

	public static class Reduce extends Reducer<Text,Text,Text,Text> {
		HashMap<String,String> map = null;
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			super.setup(context);
			//read data to memory on the mapper.
			map = new HashMap<String,String>();
			Configuration conf = context.getConfiguration();
			//String myfilePath = conf.get("/zxw151030/input/2/userdata.txt");
			//e.g /user/hue/input/
			//Path part=new Path("/user/dingcheng/xiaodi_hadoop/userdata.txt");//Location of file in HDFS
			//Path part=new Path("xiaodi_hadoop/input/userdata.txt");
			Path part=new Path(conf.get("ARGUMENT"));
			
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] fss = fs.listStatus(part);
		    for (FileStatus status : fss) {
		        Path pt = status.getPath();
		        
		        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
		        String line;
		        line=br.readLine();
		        while (line != null){
		        	String[] arr=line.split(",");
		        	//String joinValue = arr[1]+" "+arr[2]+":"+arr[6];
		        	String joinValue = arr[1]+":"+arr[6];
		        	map.put(arr[0], joinValue);
		        	 line=br.readLine();
		        }
		    }
		}
		
		public void reduce(Text key, Iterable<Text> values,Context context ) throws IOException, InterruptedException {
			
			Iterator<Text> valueItem = values.iterator();
			StringBuilder sb = new StringBuilder();
			List<String> friend_name_list = new ArrayList<String>();
			HashMap<String, Integer> stateMap = new HashMap<String, Integer>(); 
			while(valueItem.hasNext()){
				String friends[] = valueItem.next().toString().split(" ");
				for(String s:friends){
					s = s.strip();
					System.out.println( "key="+key.toString() + " common friend="+s +" is it in the map = "+ map.containsKey(s));
					if(map.containsKey(s)){
						String[] friend = map.get(s).split(":"); 
						if (!stateMap.containsKey(friend[1])) {
							stateMap.put(friend[1], 1);
						}else {
							stateMap.put(friend[1],stateMap.get(friend[1])+1);
						}
							
						friend_name_list.add(friend[0]);
					}
				}
			}
			context.write(key, new Text(friend_name_list.toString()+","+stateMap.size()));
		}
	}

// Driver program
	public static void main(String[] args) throws Exception {
		// Standard Job setup procedure.
				Configuration conf = new Configuration();
//				String[] otherArgs = new GenericOptionsParser(conf, args)
//						.getRemainingArgs();
				FileSystem FS = FileSystem.get(conf);
		    	FS.delete(new Path(args[1]),true);
				String userdata = args[2];
				conf.set("ARGUMENT", userdata);

				Job job = Job.getInstance(conf, "friendpost");
				job.setJarByClass(Join.class);
				job.setMapperClass(Map.class);
				job.setReducerClass(Reduce.class);
				
				
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);
				FileInputFormat.addInputPath(job, new Path(args[0]));
				FileOutputFormat.setOutputPath(job, new Path(args[1]));
				System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}