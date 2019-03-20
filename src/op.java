import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
public class LogReducer extends
     Reducer&lt;IntWritable, IntWritable, IntWritable, IntWritable&gt; {
 
    private static Logger logger = LoggerFactory.getLogger(LogReducer.class);
 
    public void reduce(IntWritable key, Iterable&lt;IntWritable&gt; values,
         Context context) throws IOException, InterruptedException {
 
     logger.info("Reducer started");
     int sum = 0;
     for (IntWritable value : values) {
         sum = sum + value.get();
     }
     context.write(key, new IntWritable(sum));
     logger.info("Reducer completed");
 
    }
}
