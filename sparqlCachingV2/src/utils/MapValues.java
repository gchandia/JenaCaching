package utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Map that has values sorted
 * @author gch1204
 *
 */
public class MapValues {
	
	@SuppressWarnings("hiding")
	public static <String, Integer extends Comparable<? super Integer>> HashMap<String, Integer> sortByValue(Map<String, Integer> map) {
        List<Entry<String, Integer>> list = new ArrayList<>(map.entrySet());
        list.sort(Entry.comparingByValue());
        
        HashMap<String, Integer> result = new LinkedHashMap<>();
        for (Entry<String, Integer> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }
        
        return result;
    }
}
