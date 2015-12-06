import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TopKCiting extends Configured implements Tool {

public static class MapClass extends MapReduceBase
        implements Mapper<Text, Text, Text, Text> 
    {      
	public void map(Text key, Text value,OutputCollector<Text, Text> output,Reporter reporter) throws IOException {
            output.collect(value,key);
        }
    }
   public static class Reduce extends MapReduceBase
        implements Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterator<Text> values,
                           OutputCollector<Text, Text> output,
                           Reporter reporter) throws IOException {
                           
            String csv = "";
            while (values.hasNext()) {
                if (csv.length() > 0) csv += ",";
                csv += values.next().toString();
            }
            output.collect(key, new Text(csv));
        }
    }
public static class Map2Class extends MapReduceBase implements Mapper<Text, Text, Text, IntWritable> {
        
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
        public void map(Text key, Text value,
                        OutputCollector<Text, IntWritable> output,
                        Reporter reporter) throws IOException {
	String line = value.toString();
        StringTokenizer itr = new StringTokenizer(line, " \t\n\r\f,.:;?![]'");
      	while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());               
        output.collect(word,one);
}       
}       
}       
public static class Reduce2 extends MapReduceBase
        implements Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterator<IntWritable> values,
                           OutputCollector<Text, IntWritable> output,
                           Reporter reporter) throws IOException {
                int count = 0;
        	while (values.hasNext()) {
            	values.next();
            	count++;
        }
        output.collect(key, new IntWritable(count));
        }
       }

public static class Map3Class extends MapReduceBase
        implements Mapper<Text, Text, IntWritable, IntWritable> {
        
        private final static IntWritable uno = new IntWritable(1);
        private IntWritable citationCount = new IntWritable();
        
        public void map(Text key, Text value,
                        OutputCollector<IntWritable, IntWritable> output,
                        Reporter reporter) throws IOException {
                        
            citationCount.set(Integer.parseInt(value.toString()));
            output.collect(citationCount, uno);
        }
    }
    
    public static class Reduce3 extends MapReduceBase
        implements Reducer<IntWritable,IntWritable,IntWritable,IntWritable>
    {
        
        public void reduce(IntWritable key, Iterator<IntWritable> values,
                           OutputCollector<IntWritable, IntWritable>output,
                           Reporter reporter) throws IOException {
                           
            int count = 0;
            while (values.hasNext()) {
                count += values.next().get();
            }
            output.collect(key, new IntWritable(count));
        }
    }
    
public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        
        JobConf job = new JobConf(conf, TopKCiting.class);
        
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);
        
        job.setJobName("Chain Citation Count");
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);
        
        job.setInputFormat(KeyValueTextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        JobClient.runJob(job);
        

	Configuration conf2 = getConf();        
        JobConf job2 = new JobConf(conf2, TopKCiting.class);
        //Path in = new Path(args[0]);
        Path out2 = new Path(args[2]);
        FileInputFormat.setInputPaths(job2, out);
        FileOutputFormat.setOutputPath(job2, out2);
        
        job.setJobName("Chain Citation Count");
        job.setMapperClass(Map2Class.class);
        job.setReducerClass(Reduce2.class);
        
        job.setInputFormat(KeyValueTextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
  
        JobClient.runJob(job2);
	FileSystem.get(conf).delete(out,true);


        Configuration conf3 = getConf();        
        JobConf job3 = new JobConf(conf3, TopKCiting.class);
        //Path in = new Path(args[0]);
        Path out3 = new Path(args[3]);
        FileInputFormat.setInputPaths(job3, out);
        FileOutputFormat.setOutputPath(job3, out2);
        
        job.setJobName("Chain Citation Count");
        job.setMapperClass(Map3Class.class);
        job.setReducerClass(Reduce3.class);
        
        job.setInputFormat(KeyValueTextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        JobClient.runJob(job3);
	FileSystem.get(conf2).delete(out2,true);
        return 0;
    }
    public static void main(String[] args) throws Exception { 
        int res = ToolRunner.run(new Configuration(), new TopKCiting(), args);
        
        System.exit(res);
    }            
}

