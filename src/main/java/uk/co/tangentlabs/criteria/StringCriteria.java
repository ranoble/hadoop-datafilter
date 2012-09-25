package uk.co.tangentlabs.criteria;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.MapWritable;


public class StringCriteria implements Criteria {
	List<String> values;
	String attribute;
	
	public StringCriteria(String attribute, List<String> list){
		this.attribute = attribute;
		this.values = list;
	}

	public boolean matches(MapWritable record) {
		if (values.contains(record.get(this.attribute)))
			return true;
		return false;
	}

}
