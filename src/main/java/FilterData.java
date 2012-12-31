import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import uk.co.tangentlabs.criteria.Criteria;
import uk.co.tangentlabs.criteria.StringCriteria;
import uk.co.tangentlabs.hadoop.output.JSONOutput;

import com.json.JSONArray;
import com.json.JSONException;
import com.json.JSONObject;

public class FilterData {

	private static Map<String, List<Criteria>> _checks = null;

	public FilterData() {

	}

	public static List<Criteria> getChecks(String key) {

		Map<String, List<Criteria>> _checks = new HashMap<String, List<Criteria>>();

		String[] vals = { "A" };
		List<String> values = Arrays.asList(vals);
		StringCriteria crit = new StringCriteria("mosaic", values);
		List<Criteria> criteriaList = new ArrayList<Criteria>();
		criteriaList.add(crit);

		return criteriaList;
	}

	public static String getKeyAttribute() {
		return "wpc";
	}

	public static class DataFilterMapper extends
			Mapper<Object, Text, Text, MapWritable> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			try {

				String key_attr = FilterData.getKeyAttribute();
				JSONArray json = new JSONArray(value.toString());
				for (int i = 0; i < json.length(); i++) {
					MapWritable rowMap = new MapWritable();
					JSONObject row = json.getJSONObject(i);
					Text _key = new Text(row.optString(key_attr, ""));
					Iterator<String> objectkeys = row.keys();

					while (objectkeys.hasNext()) {
						Text fieldKey = new Text(objectkeys.next());
						Text fieldKalue = new Text(row.get(fieldKey.toString())
								.toString());
						rowMap.put(fieldKey, fieldKalue);
					}

					context.write(_key, rowMap);
				}

			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static class CriteriaResolverReducer extends
			Reducer<Text, MapWritable, Text, MapWritable> {
		public void reduce(Text key, Iterable<MapWritable> values,
				Context context) throws IOException, InterruptedException {

			List<Criteria> critList = FilterData.getChecks(key.toString());
			if (critList == null)
				critList = new ArrayList<Criteria>();
			for (MapWritable value : values) {
				boolean passed = true;
				for (Criteria crit : critList) {
					if (!crit.matches(value)) {
						passed = false;
					}
				}
				if (passed) {
					context.write(key, value);
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
		Job job = new Job(conf, "Example Hadoop Data Filter Job");
		job.setJarByClass(FilterData.class);
		job.setMapperClass(DataFilterMapper.class);
		job.setReducerClass(CriteriaResolverReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MapWritable.class);
		job.setOutputFormatClass(JSONOutput.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}