package Assignment1;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class program2{
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			String[] line = value.toString().split(",");;
			InputData d = new InputData();
			d.setWord(new Text(line[0]));
			d.setField1(new IntWritable(Integer.parseInt(line[1])));
			d.setField2(new IntWritable(Integer.parseInt(line[2])));
			d.setField3(new IntWritable(Integer.parseInt(line[3])));
			d.setField4(new IntWritable(Integer.parseInt(line[4])));
			Text w = new Text();
			w.set(d.getWord());
			output.collect(w,d.getField3());
			}
	}
		public static class Reduce extends MapReduceBase implements Reducer<Text,IntWritable,Text,Text> {
			public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {
				int j;
				int min = Integer.MIN_VALUE;
				int max = Integer.MAX_VALUE;
				int sum = 0;
				int itr;
				for(j=0;values.hasNext();j++)
				{
					itr = values.next().get();
					if(j==0)
					{
						min = itr;
						max = itr;
					}
					if(min>itr)
					{
						min = itr;
					}
					if(max<itr)
					{
						max = itr;
					}
					sum = sum + itr;
				}
				int field4_avg = sum/j;
				String outp = min + "," + max + "," + field4_avg;
				Text t = new Text();
				t.set(outp.toString());
				output.collect(key,t);
			}
		}
		public static void main(String[] args) throws Exception{
			JobConf conf = new JobConf(program2.class);
			conf.setJobName("Program2");

			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);

			conf.setMapOutputKeyClass(Text.class);
			conf.setMapOutputValueClass(IntWritable.class);
			
			conf.setMapperClass(Map.class);
			conf.setReducerClass(Reduce.class);

			//conf.setInputFormat(TextInputFormat.class);
			//conf.setOutputFormat(TextOutputFormat.class);

			FileInputFormat.setInputPaths(conf, new Path(args[0]));
			FileOutputFormat.setOutputPath(conf, new Path(args[1]));

			JobClient.runJob(conf);
		}

	}
