package Hadoop.SequenceCreator;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

@SuppressWarnings("rawtypes")
public class SequenceFileWriterMapper extends Mapper {

   @SuppressWarnings("unchecked")
protected void map(Text key, Text value,@SuppressWarnings("rawtypes") Context context) throws IOException, InterruptedException {
           context.write(key, value);                
   }
    
}