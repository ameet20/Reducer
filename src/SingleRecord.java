import java.io.IOException;
import java.text.ParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.sreejith.loganalyzer.ParseLog;
 
public class LogMapper extends
     Mapper&lt;LongWritable, Text, IntWritable, IntWritable&gt; {
 
    private static Logger logger = LoggerFactory.getLogger(LogMapper.class);
    private IntWritable hour = new IntWritable();
    private final static IntWritable one = new IntWritable(1);
    private static Pattern logPattern = Pattern
         .compile("([^ ]*) ([^ ]*) ([^ ]*) \\[([^]]*)\\]"
                 + " \"([^\"]*)\""
                 + " ([^ ]*) ([^ ]*).*");
 
    public void map(LongWritable key, Text value, Context context)
         throws InterruptedException, IOException {
     logger.info("Mapper started");
     String line = ((Text) value).toString();
     Matcher matcher = logPattern.matcher(line);
     if (matcher.matches()) {
         String timestamp = matcher.group(4);
         try {
             hour.set(ParseLog.getHour(timestamp));
         } catch (ParseException e) {
             logger.warn("Exception", e);
         }
         context.write(hour, one);
     }
     logger.info("Mapper Completed");
    }
}
