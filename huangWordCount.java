package chaohParse;

/**
 *  Licensed under the Apace License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */


import java.io.IOException;
import java.util.StringTokenizer;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.fs.shell.Count;
import org.apache.hadoop.io.BytesWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.WritableComparator;
//import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
//import org.junit.internal.matchers.Each;

public class huangWordCount {
	public static class WordMapper extends Mapper<Text, BytesWritable, Text, Text>{
	//private final static IntWritable one = new IntWritable(1);
	//private Text word = new Text();	

	  public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
			System.out.println("begin mapper");
			String tmp = new String( value.getBytes(), "UTF-8" );
			String text = parsexml.getContentlineFromXML(tmp);
			text = text.replaceAll( "[^A-Za-z \n]", "" ).toLowerCase();

			StringTokenizer tokenizer = new StringTokenizer( text );

	        String tmpString = new String();
	        Integer count = new Integer(0);
	        while ( tokenizer.hasMoreTokens() ){
	        	tmpString = key.toString() + "-" + tokenizer.nextToken();
	        	context.write(new Text(tmpString), new Text(count.toString()));
	        	count += tmpString.length()+1;
	        }
	  }
  }
  
  public static class WordCombiner extends Reducer<Text, Text, Text, Text> {
	  private Text outputkeyText = new Text();
	  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		  //System.out.println("begin combine");
		  
		  String sum = new String();
		  String[] tmpString = key.toString().split("-");
		  for (Text val : values) {
			  sum += val.toString()+",";
		  }
		  outputkeyText = new Text(tmpString[1]);
		  //System.out.println(outputkeyText + "---" + tmpString[0]+":"+sum);
		  context.write(outputkeyText, new Text(tmpString[0]+":"+sum));	 
	  }
  }
  
  
  public static class WordReducer extends Reducer<Text, Text, Text, Text> {
	  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		  //System.out.println("begin reduce");
		  
		  String sum = new String();
		  System.out.println("reduce input key:" + key + values.toString());
		  for (Text val : values) {
			  sum += "["+val.toString()+"]";
		  }
		  //System.out.println(key.toString()+"---"+sum);
		  context.write(key, new Text(sum));
		  
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
    
    job.setJarByClass(huangWordCount.class);
    
    job.setMapperClass(WordMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    
    job.setCombinerClass(WordCombiner.class);
    job.setReducerClass(WordReducer.class);

    job.setInputFormatClass(ZipFileInputFormat.class);  

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
