import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import uk.co.tangentlabs.hadoop.output.JSONOutput;
import uk.co.tangentlabs.hadoop.output.TSVOutput;

import com.json.JSONArray;
import com.json.JSONException;
import com.json.JSONObject;

import com.google.common.base.Functions;
import com.google.common.collect.Ordering;

public class Translator {
	public Translator() {

	}
	
	public static class Cleaner {
		private final static Locale locale_lt = new Locale("lt", "LT");
		
		public static String clean_english(String input) {
			String[] stopwords = new String[] {
			    "the", "a", "an", "of", "to", "for",
			    "and", "is", "in", "at", "by",
			    "minutes", "session", "sitting"
			};
			String output = input.replaceAll("[^A-Za-z]*", "").toLowerCase();
			for (String stopword : stopwords) {
				if (output.equals(stopword)) {
					return "";
				}
			}
			return output;
		}

		public static String clean_lithuanian(String input) {
			String output = input.replaceAll("[^A-Za-zŽžČčŠšĖėĄąĘęĮįǪǫŲųŪū ]*", "").toLowerCase(locale_lt);
			return output;
		}
	}
	
	public static class WordCorrelationMapper extends Mapper<Object, Text, Text, Text> {
		private Text word_key = new Text();
		private Text word_value = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			try {

				// each input line is two strings separated by a single tab
				String line = value.toString();
				String[] languages = line.split("\t");
				String english = languages[0];
				String lithuanian = Cleaner.clean_lithuanian(languages[1]);
				
				for (String lithuanian_word : lithuanian.split(" ")) {
					if (!lithuanian_word.isEmpty()) {
						word_key.set(lithuanian_word);
						word_value.set(english);
						context.write(word_key, word_value);
					}
				}
			}  catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static class CountReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			// @todo there is a more efficient way to do this pretty sure

			Map<String, Integer> counts = new HashMap<String, Integer>();

			for (Text english : values) {
				for (String english_word : english.toString().split(" ")) {
					String cleaned_english_word = Cleaner.clean_english(english_word);
					if (!cleaned_english_word.isEmpty()) {
						Integer count = 1;
						
						if (counts.containsKey(cleaned_english_word)) {
							count = counts.get(cleaned_english_word) + 1;
						}
	
						counts.put(cleaned_english_word,  count);
					}
				}
			}
			
			if (!counts.isEmpty()) {
				Entry<String,Integer> maxEntry = null;
				for(Entry<String,Integer> entry : counts.entrySet()) {
					if (entry.getValue() < 100) {
					    if (maxEntry == null || entry.getValue() > maxEntry.getValue()) {
					        maxEntry = entry;
					    }
					}
				}
				
				if (maxEntry.getValue() > 30) {
					Text best_match = new Text();
					best_match.set(maxEntry.getKey());
					context.write(key, best_match);
				}
			}
		}
	}

	/**
	 * The main entry point.
	 */
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		Job job = new Job(conf, "Example Hadoop Wordcount Job");
		job.setJarByClass(Translator.class);

		job.setMapperClass(WordCorrelationMapper.class);
		job.setReducerClass(CountReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setOutputFormatClass(TSVOutput.class);
		// job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}