import java.io.I0Exception;

import org-apache. hadoop. 20.Longritable:

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

public class LogsCounterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

private final static IntWritable ONE = new IntWritable(1);

private Text logType = new Text();

@override

public void map(Longwritable key, Text value, Context context)

throws IOException, InterruptedException {

String line = value.toString();

if (line.contains ("wallet-rest-api")){

if (line.contains("[INFO]")*

logType.set ("INFO");

Jelse if (line.contains(" [SEVERE]")){

logType.set ("SEVERE") ;

Jelse if (line.contains("[WARN]")){

logType.set ("WARN" );

｝

context.write(logType, ONE);

｝

｝

}