import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.File0utputFormat;

import org.apache.hadoop.util.Tool;

import org.apache.hadoop.util.ToolRunner;

public class ContadorDriver extends Configured implements Toolf

public static void main (String(] args) throws Exception {

int res= ToolRunner.run (new ContadorDriver(), args);

System. exit (res);

}

public int run (String(] args) throws Exception 1

Job job = Job.getInstance(getConf(), "Contador de Logs");|

job.setJarByClass (this.getClass());

FileInputFormat.addInputPath(job, new Path(args [0]));

FileOutputFormat.setOutputPath(job, new Path(args (1]));

job.setMapperClass(LogsCounterMapper.class);

job. setReducerClass(ContadorReducer.class);

job.setOutputKeyClass(Text.class);

job.setOutputValueClass (IntWritable.class);

return job.waitForCompletion(true) ? 0 : 1;

｝

}

