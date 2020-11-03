package jcn;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

import com.google.re2j.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.tools.GetConf;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.SequenceFileInputFilter;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.InverseMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.zookeeper.txn.Txn;

import sun.tools.asm.CatchData;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;


public class WordCount{

  public static class TokenizerMapper extends
      Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    //2.0

    private Set<String> patternsToSkip = new HashSet<>(); //patternsToSkip
    private Configuration conf;
    private BufferedReader fis;

    public boolean isDigit(String str){
      return str.matches("[0-9]{1,}");
    }

    private void parseSkipFile(String fileName){
      try{
        this.fis = new BufferedReader(new FileReader(fileName));
        String pattern = null;
        while((pattern = this.fis.readLine())!=null)
          this.patternsToSkip.add(pattern);
        }catch(IOException ioe){
          System.err.println("Caught exception while parsing the cached file '" + 
          StringUtils.stringifyException(ioe));
        }
    }
    
  

    public void setup(Context context) throws IOException, InterruptedException{
      this.conf= context.getConfiguration();
      if(this.conf.getBoolean("wordcount.skip.patterns", false)){
        URI[] patternsURIs = Job.getInstance(this.conf).getCacheFiles();
        for(URI pattersURI : patternsURIs){
          Path patternsPath = new Path(pattersURI.getPath());
          String pattersFileName = patternsPath.getName().toString();
          parseSkipFile(pattersFileName);
        }
      }
    }

    
    //2.0
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {

      String line = value.toString().toLowerCase();
      line  = line.replaceAll("\\d+", "");
      for (String pattern : this.patternsToSkip){
        if(pattern.contains("\\")){
          line = line.replaceAll(pattern, "");
        }
      }
      StringTokenizer itr = new StringTokenizer(line);
      while(itr.hasMoreTokens()){
        boolean flag = true;
        String tmp = itr.nextToken();
        for (String pattern : patternsToSkip){
          if(pattern.matches(tmp)){
            flag = false;
          }
        }
        if(flag){
          word.set(tmp);
          if(word.getLength()>3 && !isDigit(word.toString())){
            context.write(word, one);
            //Counter counter = context.getCounter(CountersEnum.class.getName(), CountersEnum.INPUT_WORDS.toString());
            //counter.increment(1L);
          }
        }
      }
    }
  }

    

  private static class IntWritableDecreasingComparator extends IntWritable.Comparator {
      public int compare(WritableComparable a, WritableComparable b) {
          //System.out.println("ss");
          return -super.compare(a, b);
      }

      public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
          //System.out.println("ss1");
          return -super.compare(b1, s1, l1, b2, s2, l2);
      }
  }

  public static class InverseMapper<K, V> extends Mapper<K,V,V,K> {  
    // The inverse function.  Input keys and values are swapped.
    @Override  
    public void map(K key, V value, Context context  
                    ) throws IOException, InterruptedException {  
      context.write(value, key);  
    }
}

public static class SortReducer extends Reducer<IntWritable, Text, Text, NullWritable>{
  private Text result = new Text();
  int i = 0;
  @Override
  protected void reduce(IntWritable key, Iterable<Text> values, Reducer<IntWritable,Text,Text,NullWritable>.Context context)throws IOException,InterruptedException {
    for (Text val:values){
      if(i>=100) break;
      i++;
      result.set(val.toString());
      String str = "排名"+i+":"+result+",共计"+key+"次";
      context.write(new Text(str), NullWritable.get());
    }
  }
}

  
  public static class IntSumReducer extends
      Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {//遍历values，得到同一key的所有value
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);//输出对<key, value>
    }
  }



public static void main(String[] args) throws Exception {
  Configuration conf = new Configuration();
  GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
  String[] remainingArgs = optionParser.getRemainingArgs();
  if (remainingArgs.length == 2 && remainingArgs.length == 4) {
    System.err.println("Usage: wordcount <in> <out> [-skip skipPatternFile]");
    System.exit(2);
  } 

  //2.0
  Path tempDir = new Path("wordcount-tmp-output");

  Job job = Job.getInstance(conf, "word count");
  job.setJarByClass(WordCount.class);
  job.setMapperClass(TokenizerMapper.class);
  job.setCombinerClass(IntSumReducer.class);
  job.setReducerClass(IntSumReducer.class);
  job.setOutputKeyClass(Text.class); 
  job.setOutputValueClass(IntWritable.class);
  job.setOutputFormatClass(SequenceFileOutputFormat.class);
  List<String> otherArgs = new ArrayList<>();
  for (int i = 0; i < remainingArgs.length; i++) {
    if ("-skip".equals(remainingArgs[i])) {
      job.addCacheFile((new Path(remainingArgs[++i])).toUri());
      job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
    } else {
      otherArgs.add(remainingArgs[i]);
    } 
  } 
  FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
  FileOutputFormat.setOutputPath(job, tempDir);//2.0


  //2.0
  //新建一个job,把上一个WordCount这个job的输出（存在tempDir里）当作输入
  job.waitForCompletion(true);

  Job sortjob =Job.getInstance(conf,"sort");
  FileInputFormat.addInputPath(sortjob, tempDir);
  sortjob.setJarByClass(WordCount.class);
  sortjob.setInputFormatClass(SequenceFileInputFormat.class);
  sortjob.setMapperClass(InverseMapper.class);
  sortjob.setNumReduceTasks(1);

  //add
  sortjob.setReducerClass(SortReducer.class);
  
  FileOutputFormat.setOutputPath(sortjob, new Path(otherArgs.get(1)));
  sortjob.setOutputKeyClass(IntWritable.class);
  sortjob.setOutputValueClass(Text.class);
  sortjob.setSortComparatorClass(IntWritableDecreasingComparator.class);

  sortjob.waitForCompletion(true);

  FileSystem.get(conf).delete(tempDir);
  System.exit(0);


  
}


}






