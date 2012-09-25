package uk.co.tangentlabs.hadoop.output;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.json.JSONException;
import com.json.JSONObject;


public class JSONRecordWriter extends RecordWriter<Text, MapWritable>{
		
		private static final String utf8 = "UTF-8";

	    private DataOutputStream out;

	    public JSONRecordWriter(DataOutputStream out) throws IOException {
	      this.out = out;
	      
	    }

		@Override
		public void close(TaskAttemptContext arg0) throws IOException,
				InterruptedException {
			out.close();			
		}

		@Override
		public void write(Text arg0, MapWritable _map) throws IOException,
				InterruptedException {
			JSONObject object = new JSONObject();
			Set<Writable> keys = _map.keySet();
			for (Writable key: keys){
				try {
					object.put(key.toString(), _map.get(key).toString());
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
			out.write(object.toString().getBytes(utf8));
		}
		
	}
