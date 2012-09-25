package uk.co.tangentlabs.criteria;
import java.util.Map;

import org.apache.hadoop.io.MapWritable;


public interface Criteria {
	public boolean matches(MapWritable value);
}
