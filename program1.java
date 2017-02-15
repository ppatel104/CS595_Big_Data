package Assignment1;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class program1 {
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, InputData> {

		public void map(LongWritable key, Text value, OutputCollector<Text, InputData> output, Reporter reporter) throws IOException {
			String[] line = value.toString().split(",");;
			InputData d = new InputData();
			d.setWord(new Text(line[0]));
			d.setField1(new IntWritable(Integer.parseInt(line[1])));
			d.setField2(new IntWritable(Integer.parseInt(line[2])));
			d.setField3(new IntWritable(Integer.parseInt(line[3])));
			d.setField4(new IntWritable(Integer.parseInt(line[4])));
			Text word = new Text();
			word.set(d.toString());
			output.collect(word,d);
			}
	}
		public static class Reduce extends MapReduceBase implements Reducer<Text,InputData,Text,Text> {
			public void reduce(Text key, Iterator<InputData> values, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {
			for(int i=0;values.hasNext();i++)
			{
			InputData id1 = new InputData();
			id1 = values.next();
			if(id1.getField1().get()<id1.getField3().get() && id1.getField2().get()>id1.getField4().get())
			{
				output.collect(key,new Text());
			}
			}
			
			}
		}
		public static void main(String[] args) throws IOException{
			JobConf conf = new JobConf(program1.class);
			conf.setJobName("Program1");

			conf.setMapOutputKeyClass(Text.class);
			conf.setMapOutputValueClass(InputData.class);
			
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);

			conf.setMapperClass(Map.class);
			conf.setReducerClass(Reduce.class);

			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);

			FileInputFormat.addInputPath(conf, new Path(args[0]));
			FileOutputFormat.setOutputPath(conf, new Path(args[1]));

			JobClient.runJob(conf);
		}

	}
