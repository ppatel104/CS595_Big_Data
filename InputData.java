package Assignment1;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class InputData implements Writable{
	Text word;
	IntWritable field1;
	IntWritable field2;
	IntWritable field3;
	IntWritable field4;
	public InputData()
	{
		word = new Text();
		field1 = new IntWritable();
		field2 = new IntWritable();
		field3 = new IntWritable();
		field4 = new IntWritable();
	}
	public InputData(Text aword,IntWritable afield1,IntWritable afield2,IntWritable afield3,IntWritable afield4)
	{
		this.word = aword;
		this.field1 = afield1;
		this.field2 = afield2;
		this.field3 = afield3;
		this.field4 = afield4;
	}
	public Text getWord() {
		return word;
	}
	public void setWord(Text word) {
		this.word = word;
	}
	public IntWritable getField1() {
		return field1;
	}
	public void setField1(IntWritable field1) {
		this.field1 = field1;
	}
	public IntWritable getField2() {
		return field2;
	}
	public void setField2(IntWritable field2) {
		this.field2 = field2;
	}
	public IntWritable getField3() {
		return field3;
	}
	public void setField3(IntWritable field3) {
		this.field3 = field3;
	}
	public IntWritable getField4() {
		return field4;
	}
	public void setField4(IntWritable field4) {
		this.field4 = field4;
	}
	public String toString(){
		return word+","+field1+","+field2+","+field3+","+field4;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		word.readFields(in);
		field1.readFields(in);
		field2.readFields(in);
		field3.readFields(in);
		field4.readFields(in);
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		word.write(out);
		field1.write(out);
		field2.write(out);
		field3.write(out);
		field4.write(out);
	}
	
}
