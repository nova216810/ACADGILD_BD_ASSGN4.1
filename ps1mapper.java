
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public  class ps1mapper extends Mapper<LongWritable, Text, Text, Text>
{
	private Text record = new Text();
	private Text dummy = new Text();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
	      
		System.out.println("Input key = " + key.toString() + " Input value = " + value.toString());
	     
		String[] token = value.toString().split("\\|");

			int flag=0;
			System.out.println("company "+ token[0]+" product "+token[1] );
			if (token[0].equals("NA") || token[1].equals("NA"))
			{
				flag = 1;
			}
			
			if (flag == 0)
			{
				record .set(value);
				dummy.set("");
				context.write(dummy, record);
			 	System.out.println(record.toString() );
			}
		 } 
		}
	    
	
	  

	
