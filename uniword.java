/**
 *  Licensed under the Apache License, Version 2.0 (the "License");
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

public class uniword {

	//enum Counter {LINESKIP,}
	public static class TokenizerMapper extends Mapper<Text, BytesWritable, Text, Text>{
    
		//private final static Tte one = new IntWritable(1);
		private Text keyInfo = new Text();
		//private Text valueInfo = new Text();
		private Stemmer s = new Stemmer();
		
		public void map(Text key, BytesWritable value, Context context) throws  IOException, InterruptedException {
			String tmp = new String( value.getBytes(), "UTF-8" );
			String text = parsexml.getContentlineFromXML(tmp).replaceAll( "[^A-Za-z \n]", "" ).toLowerCase();
			StringTokenizer tokenizer = new StringTokenizer( text );
	        while ( tokenizer.hasMoreTokens() ){
	        	s.add(tokenizer.nextToken());
	        	s.stem();
	        	keyInfo.set(s.toString());
	        	context.write(keyInfo,key);
	        }
		}
	}
	/*
	public static class myUniwordCombiner extends Reducer<Text, Text, Text, Text>
	{	
		private Text KeyInfo = new Text();
		private Text valueInfo = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{	
			  String[] tmpString = key.toString().split("-");
			  //int count = 0;
			  
			  for (Text val : values) {
				 count += Integer.parseInt(val.toString());
			  }
			  
			  KeyInfo.set(tmpString[0]);
//			  valueInfo.set(tmpString[1]+":"+count);
			  valueInfo.set(tmpString[1]);

			  context.write(KeyInfo,valueInfo);	 
		}
	}
	*/
	public static class myUniwordReducer extends Reducer<Text,Text,Text,Text> {
		private Text valueInfo = new Text();
	    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		    StringBuffer fileList = new StringBuffer();
		    for (Text val : values) {
		      String string = val.toString();      
		      fileList.append(string+"-");
		    }
		    valueInfo.set(fileList.toString());
		    context.write(key,valueInfo);
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
    job.setJarByClass(uniword.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    
    //job.setCombinerClass(myUniwordCombiner.class);
    job.setReducerClass(myUniwordReducer.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    job.setInputFormatClass(ZipFileInputFormat.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
 }
