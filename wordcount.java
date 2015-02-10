/**
 //positional index for each passage
 */

package chaohParse;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class wordcount {

	//enum Counter {LINESKIP,}
	public static class TokenizerMapper extends Mapper<Text, BytesWritable, Text, Text>{
    private Text KeyInfo = new Text();
    private Text ValueInfo = new Text();
	private Stemmer s = new Stemmer();

	public void map(Text key, BytesWritable value, Context context)
		throws  IOException, InterruptedException {
	
		String tmp = new String( value.getBytes(), "UTF-8" );
		String text = parsexml.getContentlineFromXML(tmp).replaceAll( "[^A-Za-z \n]", "" ).toLowerCase();
		StringTokenizer tokenizer = new StringTokenizer( text );


        Integer count = new Integer(0);
        while ( tokenizer.hasMoreTokens() ){
        	s.add(tokenizer.nextToken());
        	s.stem();
        	KeyInfo.set(s.toString() + "-" + key.toString());
        	ValueInfo.set(count.toString());
        	context.write(KeyInfo, ValueInfo);
        	//count += tmpString.length()+1; // returen the postion detail of this word
        	count += 1;
        }
	}
	}
	
	public static class myCombiner extends Reducer<Text, Text, Text, Text>
	{	
		private Text KeyInfo = new Text();
	    private Text ValueInfo = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException{	
			  StringBuffer sum = new StringBuffer();
			  String[] tmpString = key.toString().split("-");
			  tmpString[1].replace("newsML.xml", "");//make the passage name shorter
			  for (Text val : values) {
				  sum.append(val.toString()+",");
			  }
			  KeyInfo.set(tmpString[0]);
			  ValueInfo.set("<"+tmpString[1]+":"+sum+">");
			  context.write(KeyInfo,ValueInfo);	 
	}  
	}
	
	public static class IntSumReducer extends Reducer<Text,Text,Text,Text> {
	    private Text ValueInfo = new Text();
	    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	    	StringBuffer fileList = new StringBuffer();
	    	for (Text val : values) {
	    	  String string = val.toString();      
	    	  fileList.append(string+"-");
	    	}
	    	ValueInfo.set(fileList.toString());
	    	context.write(key, ValueInfo);
	    }
	}

	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: wordcount <in> <out>");
	      System.exit(2);
	    }
	    Job job = new Job(conf, "word count");
	    job.setJarByClass(wordcount.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    
	    job.setCombinerClass(myCombiner.class);
	    job.setReducerClass(IntSumReducer.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    job.setInputFormatClass(ZipFileInputFormat.class);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
 }
