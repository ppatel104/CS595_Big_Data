package Assignment1;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
public class temporary implements Writable,WritableComparable<temporary>{
	Text word;
	IntWritable field4;
	List<temporary> temp = new ArrayList<temporary>();
	public temporary(){
		word  = new Text();
		field4 = new IntWritable();
	}
	public temporary(Text aword, IntWritable afield){
		this.word  = aword;
		this.field4 = afield;
	}
	public Text getword(){
		return word;
	}
	public IntWritable getfield4()
	{
		return field4;
	}
	public void setword(Text aword){
		this.word = aword;
	}
	public void setfield4(IntWritable afield4)
	{
		this.field4 = afield4;
	}
	public List<temporary> get_t(){
		return temp;
	}
	public void set_t(List<temporary> tp)
	{
	      this.temp = tp;
	}
	public void readFields(DataInput in) throws IOException {
		word.readFields(in);
		field4.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		word.write(out);
		field4.write(out);
	}

	public int compareTo(temporary t) {
		// TODO Auto-generated method stub
		return (this.getfield4().get() < t.getfield4().get() ? -1 : (this.getfield4().get() == t.getfield4().get() ? 0 : 1));

	}
	public String toString()
	{
		return word.toString()+" "+field4.toString();
	}
}
