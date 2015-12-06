import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Split extends Configured implements Tool {
    
    public static class MapClass extends MapReduceBase
        implements Mapper<LongWritable, Text, NullWritable, Text> {
        
        public void map(LongWritable key, Text value,
                        OutputCollector<NullWritable, Text> output,
                        Reporter reporter) throws IOException {
                        
            output.collect(NullWritable.get(), value);
        }
    }
    
    public static class PartitionByCountryMTOF
        extends MultipleTextOutputFormat<NullWritable,Text>
    {
        protected String generateFileNameForKeyValue(NullWritable key,
                                                     Text value,
                                                     String inputfilename)
        {
	    String[] arr = value.toString().split("/", -1);
            String country = arr[3].substring(1,12);
            return country+"/"+inputfilename;
        }
    }
    
    public int run(String[] args) throws Exception {
       
        Configuration conf = getConf();
        
        JobConf job = new JobConf(conf, Split.class);
        
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);
        
        job.setJobName("MultiFile");
        job.setMapperClass(MapClass.class);
        
        job.setInputFormat(TextInputFormat.class);
        job.setOutputFormat(PartitionByCountryMTOF.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        
        job.setNumReduceTasks(0);
        
        JobClient.runJob(job);
        
        return 0;
    }
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Split(), args);
        
        System.exit(res);
    }
}
