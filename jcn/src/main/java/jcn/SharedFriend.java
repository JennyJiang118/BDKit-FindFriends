package jcn;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.*;

import java.io.IOException;
public class SharedFriend {

	//make sure is mutual friends
	static class MutualFriendsMapper extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] userFriends = line.split(", ");
			String user = userFriends[0];
			String[] friends = userFriends[1].split(" ");
			
			for(String friend : friends){
				context.write(new Text(friend), new Text(user));//exchange key & value
			}
		}
	}
	//output:<其中一个好友，这个人>
	
	//对所有key相同的进行拼接
	static class MutualFriendsReducer extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			StringBuffer sb = new StringBuffer();
			for(Text friend : values){
				sb.append(friend.toString()).append(",");
			}
			sb.deleteCharAt(sb.length()-1);
			context.write(key, new Text(sb.toString()));
		}
	}
	//output:<一个好友，有此好友的所有人>
	
	//Find friends
	static class FindFriendsMapper extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] friendUsers = line.split("\t");
			String friend = friendUsers[0];
			String[] users = friendUsers[1].split(",");
			Arrays.sort(users); //sort 防止重复
			
			//两两配对
			for(int i=0;i<users.length-1;i++){
				for(int j=i+1;j<users.length;j++){
					context.write(new Text("(["+users[i]+","+users[j]+"],"), new Text(friend));
				}
			}
		}
	}
	

	static class FindFriendsReducer extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			StringBuffer sb = new StringBuffer();
			Set<String> set = new HashSet<String>();
			for(Text friend : values){
				if(!set.contains(friend.toString()))//排除多个共同好友的重复计数
					set.add(friend.toString());
			}
			for(String friend : set){
				sb.append(friend.toString()).append(",");
			}
			sb.deleteCharAt(sb.length()-1);
			
			context.write(key, new Text("["+sb.toString()+"])"));
		}
	}
	
	public static void main(String[] args)throws Exception {
		Configuration conf = new Configuration();
		String[] OtherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(OtherArgs.length<2){
			System.err.println("error");
			System.exit(2);
		}

		//mutual
		Job job1 = Job.getInstance(conf);
		job1.setJarByClass(SharedFriend.class);
		job1.setMapperClass(MutualFriendsMapper.class);
		job1.setReducerClass(MutualFriendsReducer.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		Path tempDir = new Path("mutual-tmp-output");
		FileInputFormat.setInputPaths(job1, new Path(OtherArgs[0]));
		FileOutputFormat.setOutputPath(job1, tempDir);
		
		job1.waitForCompletion(true);
		
		//find
		Job job2 = Job.getInstance(conf);
		job2.setJarByClass(SharedFriend.class);
		job2.setMapperClass(FindFriendsMapper.class);
		job2.setReducerClass(FindFriendsReducer.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job2, tempDir);
		FileOutputFormat.setOutputPath(job2, new Path(OtherArgs[1]));
		
		boolean res = job2.waitForCompletion(true);
		
		System.exit(res?0:1);
	}
}

