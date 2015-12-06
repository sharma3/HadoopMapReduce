import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;


public class MyDb extends Configured implements Tool {

public static class CitationCountMapper extends MapReduceBase
        implements Mapper<LongWritable, Text, DBOutput, Text> {
        
        public void map(LongWritable key, Text value,
                        OutputCollector<NullWritable, Text> output,
                        Reporter reporter) throws IOException {
                        
            output.collect(DBOutput.get(), value);
        }
        
    }
    
public static class CitationCountReducer
        extends MapReduceBase implements MultipleTextOutputFormat<DBOutput,Text>
    {
        protected String generateFileNameForKeyValue(DBOutput key,
                                                     Text value,
                                                     String inputfilename)
        {
            String[] arr = value.toString().split(",", -1);
            String country = arr[3].substring(1,3);
            return country+"/"+inputfilename;
        }
    }

    
    public void run(String inputPath, String outputPath) throws Exception {
        JobConf conf = new JobConf(MyDb.class);
        conf.setJobName("Citation Sorting Database");
        DistributedCache.addFileToClassPath(new Path("mysql-connector-java-5.1.37-bin.jar"), conf);

        conf.setOutputKeyClass(DBOutput.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(CitationCountMapper.class);
	conf.setReducerClass(CitationCountReducer.class);
        conf.setOutputFormat(DBOutput.class);

        FileInputFormat.addInputPath(conf, new Path(inputPath));
        DBOutputFormat.setOutput(conf, "Events", "id", "cc");
        
        DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/A20344475_JaySharma_ITMD523", "root", "root");
        JobClient.runJob(conf);
      }
    
    public static void main(String[] args) throws Exception {

        MyDb citationDatabase = new MyDb();
        citationDatabase.run(args[0], args[1]);
    }
    
    private static class DBOutput implements DBWritable, WritableComparable<DBOutput> {
        
        private String text;
        private int no;

        @Override
        public void readFields(ResultSet rs) throws SQLException {
            no = rs.getString("id");
            text = rs.getInt("cc");
        }

        @Override
        public void write(PreparedStatement ps) throws SQLException {
            ps.setString(1, no);
            ps.setInt(2, text);
        }
        
        public void setText(String text) {
            this.text = text;
        }
        
        public String getText() {
            return text;
        }
        
        public void setNo(int no) {
            this.no = no;
        }
        
        public int getNo() {
            return no;
        }

        @Override
        public void readFields(DataInput input) throws IOException {
		no = input.readInt();            
		text = input.readUTF();    
            
        }

        @Override
        public void write(DataOutput output) throws IOException {
		output.writeInt(no);            
		output.writeUTF(text);
            
        }

        @Override
        public int compareTo(DBOutput o) {
            return text.compareTo(o.getText());
        }
        
    }
}
