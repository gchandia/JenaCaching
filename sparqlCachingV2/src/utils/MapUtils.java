package utils;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

public class MapUtils {
	
	public static int getMax(Collection<Integer> c) {
		Iterator<Integer> col = c.iterator();
		int max = col.next();
		while(col.hasNext()) {
			int next = col.next();
			if (next > max) max = next;
		}
		return max;
	}
	
	public static String getKey(HashMap<String, Integer> c, Integer i) {
		String v = "";
		for (Entry<String, Integer> e : c.entrySet()) {
			if (e.getValue().equals(i)) v = e.getKey();
		}
		return v;
	}
}
