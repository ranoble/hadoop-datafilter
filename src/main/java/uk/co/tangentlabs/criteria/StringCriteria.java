package uk.co.tangentlabs.criteria;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


public class StringCriteria implements Criteria {
	List<String> values;
	Text attribute;
	
	public StringCriteria(String attribute, List<String> list){
		this.attribute = new Text(attribute);
		this.values = list;
	}

	public boolean matches(MapWritable record) {
		Writable attributeValue = record.get(this.attribute);
		if (values.contains(attributeValue.toString()))
			return true;
		return false;
	}

}
