import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import uk.co.tangentlabs.criteria.Criteria;

import com.json.JSONException;
import com.json.JSONObject;


public class testCriteria {
	@Test
	public void testCriteria() throws JSONException {

	   // MyClass is tested
	   FilterData tester = new FilterData();
	   //System.out.println(tester.getChecks(""));
	   /*
	    * {"wpc": "AAA","name": "bob","mosaic": "A","id": 1}
	    */
	   MapWritable rowMap = new MapWritable();
	   JSONObject row = new JSONObject("{\"wpc\": \"AAA\",\"name\": \"bob\",\"mosaic\": \"A\",\"id\": 1}");
	   Iterator<String> objectkeys = row.keys();

	   while (objectkeys.hasNext()) {
			Text fieldKey = new Text(objectkeys.next());
			Text fieldKalue = new Text(row.get(fieldKey.toString())
					.toString());
			rowMap.put(fieldKey, fieldKalue);
	   }
	   List<Criteria> checks = tester.getChecks("");
	   assertEquals(checks.size(), 1);
	   
	   Criteria check = checks.get(0);
	  
	   assertTrue(check.matches(rowMap));
	   
	   rowMap = new MapWritable();
	   row = new JSONObject("{\"wpc\": \"AAA\",\"name\": \"bob\",\"mosaic\": \"B\",\"id\": 1}");
	   objectkeys = row.keys();
	   while (objectkeys.hasNext()) {
			Text fieldKey = new Text(objectkeys.next());
			Text fieldKalue = new Text(row.get(fieldKey.toString())
					.toString());
			rowMap.put(fieldKey, fieldKalue);
	   }
	   assertFalse(check.matches(rowMap));
	   
	 } 
}
