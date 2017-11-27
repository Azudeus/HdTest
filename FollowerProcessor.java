package FollowerProcessor;

import java.io.IOException;
import java.util.*;
import java.lang.Iterable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import utils.MiscUtils;

//Reference: https://github.com/andreaiacono/MapReduce/blob/master/src/main/java/samples/topn/TopN.java
//Reference: https://stackoverflow.com/questions/23042829/getting-java-heap-space-error-while-running-a-mapreduce-code-for-large-dataset

public class FollowerProcessor {
  
  //Mapper
  public static class MapFollower extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{
    public void map(LongWritable key, Text value, OutputCollector<Text,Text> output, Reporter reporter) throws IOException{
      String[] rows = value.toString().split("\r?\n");
      for(int i=0;i<rows.length;i++){
        String key=cols[0];
        String[] cols = rows[i].toString().split(",");
        String anchor=cols[1];
        output.collect(new Text(key),new Text(anchor));

        for(int j=i+1;j<rows.length;j++){
          String secondaryKey=cols[0];
          if (secondaryKey==anchor){
            //Generate the follower of anchor
            String[] cols = rows[j].toString().split(",");
            String value=cols[1];
            output.collect(new Text(key),new Text(value));
          }
        }
      }
    }

  }

  //Remove Duplication
  //To Be Implemented

  //Reducer
  public static class ReduceFollower extends MapReduceBase implements Reducer<Text, Text, Text, Text>{  

    private Map<Text, IntWritable> countMap = new HashMap<>();
    
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      int count = 0;
      while (values.hasNext()) {
        count++;
      }
      countMap.collect(key, new IntWritable(count));
    }  

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      Map<Text, IntWritable> sortedMap = MiscUtils.sortByValues(countMap);

      int counter = 0;
      for (Text key: sortedMap.keySet()) {
        if (counter ++ == 10) {
          break;
        }
        context.write(key, sortedMap.get(key));
      }
    }

  }

  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(FollowerProcessor.class);
    conf.setJobName("Follower Processor");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapperClass(MapFollower.class);
    conf.setReducerClass(ReduceFollower.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    JobClient.runJob(conf);
  }
  
}

