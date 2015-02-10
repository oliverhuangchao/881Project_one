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

public class biwordsCount {

	public static class TokenizerMapper extends Mapper<Text, BytesWritable, Text, Text>{
    
	public void map(Text key, BytesWritable value, Context context)
		throws  IOException, InterruptedException {
	
		//System.out.println("begin mapper");
		String tmp = new String( value.getBytes(), "UTF-8" );
		String text = parsexml.getContentlineFromXML(tmp);
		text = text.replaceAll( "[^A-Za-z \n]", "" ).toLowerCase();

		StringTokenizer tokenizer = new StringTokenizer( text );

        String tmpString = new String();
        Integer count = new Integer(0);
        String outputString = new String();

        String currentString = tokenizer.nextToken();
        String nextString = new String();
        
        while ( tokenizer.hasMoreTokens() ){
        	nextString = tokenizer.nextToken();
        	outputString = currentString + "$$" + nextString;
        	tmpString = outputString + "-" + key.toString() ;
        	context.write(new Text(tmpString), new Text(count.toString()));
        	count += currentString.length()+1;
        	currentString = nextString;
        }

	}
	}
	
	public static class myCombiner extends Reducer<Text, Text, Text, Text>
	{	
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException{
			
			
			 StringBuffer sum = new StringBuffer();
			  String[] tmpString = key.toString().split("-");
			  //int count = 0;
			  for (Text val : values) {
				  //count++;
				  sum.append(val.toString()+",");
			  }
			  
			  context.write(new Text(tmpString[0]), new Text("["+tmpString[1]+":"+sum+"]"));	 
			
		}  
	}
	
	public static class IntSumReducer 
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();
    
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	
    	
    	//System.out.println("begin reduce");
    	//int count =0;
    	StringBuffer fileList = new StringBuffer();
    	//StringBuffer fileList = new StringBuffer();
      for (Text val : values) {
    	  String string = val.toString();
          
    	  fileList.append(string);
      }
      result.set(fileList.toString());
      context.write(key, result);
  	
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
    job.setJarByClass(biwordsCount.class);
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
