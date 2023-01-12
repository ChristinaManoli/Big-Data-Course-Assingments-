
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Project extends Configured implements Tool{
	
	//The following code shows the Mapper class and the map function.
	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>
	{
	    private Text ip_filename = new Text();
	    private Text date = new Text();
	    
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	    {
			
	    	//Split up (tokenize) a line into each variable (tokens) according to the delimiter ","
			String[] contents = value.toString().split(",", 15);
			
			if (!contents[0].equals("ip"))
			{
				//check if the filename is missing and only the file extension is present
				if (contents[6].charAt(0)=='.')
				{
					contents[6]=contents[5]+contents[6]; //then the filename is the document accession number.
				}

				ip_filename.set(String.join("!", contents[0], contents[6])); //The variables ip and file_name are set as the key (separated by "!")
				date.set(contents[1]); //and the date is set as the value
				context.write(ip_filename, date);		
			}
		}
	    
	}
	
		
	public static class TheReducer extends Reducer<Text, Text, Text, Text>
	{
		private Text filename = new Text();
	    private Text ip = new Text();

    	
	    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	    {
	    	Set<String> set = new HashSet<String> ();

	    	for (Text val : values)
			{
	    		set.add(val.toString());  //Putting each value into a Hashset in order to eliminate the duplicates
			}
	    	if (set.size()>1)  //if the unique remaining dates are more than 1 then
	    	{
				String[] contents2 = key.toString().split("!"); //we split the key into the respective ip and filename
				ip.set(contents2[0]);
				filename.set(contents2[1]);
	    		context.write(ip, filename);
	    	}
	    }
	    
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Project(), args);
	    System.exit(res);
	  }
	
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "User accessed the file");
		job.setJarByClass(Project.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(TheReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	  }
}
