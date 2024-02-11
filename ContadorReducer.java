import java.io.I0Exception;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

public class ContadorReducer extends Reducer<Text, Intwritable, Text, Intwritable> i

//private final static DoubleWritable result = DoubleWritable();

@Override

public void reduce(Text key, Iterable<IntWritable> values, Context context)

throws IOException, InterruptedException {

int sum=0;||

for (IntWritable value : values) {

sum += value.get);

｝

//result.set (sum) ;

context.write(key,new IntWritable(sum));

}

}