package data_reader;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import org.junit.jupiter.params.shadow.com.univocity.parsers.tsv.TsvParser;
import org.junit.jupiter.params.shadow.com.univocity.parsers.tsv.TsvParserSettings;

public class TopNData {
	private String[][] sortedData;
	private HashMap<String, Integer> allValues;
	
	public TopNData(int amount) {
		this.sortedData = new String[amount][2];
		for (int i = 0; i < this.sortedData.length; i++) {
			this.sortedData[i][0] = "";
			this.sortedData[i][1] = "0";
			this.allValues = new HashMap<String, Integer>();
		}
	}
	
	private void insertData(String[] bgp) {
		if (this.allValues.get(bgp[0]) == null) this.allValues.put(bgp[0], Integer.parseInt(bgp[1]));
		else this.allValues.replace(bgp[0], this.allValues.get(bgp[0]) + Integer.parseInt(bgp[1]));
		
		/*for (int i = 0; i < this.sortedData.length; i++) {
			if (Integer.parseInt(bgp[1]) > Integer.parseInt(this.sortedData[i][1])) {
				this.sortedData[i] = bgp;
				break;
			}
		}*/
	}
	
	public void retrieveTopN(File dir) {
		TsvParserSettings settings = new TsvParserSettings();
		TsvParser parser;
		
		assert(dir.isDirectory());
		for (File file : dir.listFiles()) {
			parser = new TsvParser(settings);
			List<String[]> allRows = parser.parseAll(file);
			for (String[] l : allRows) {
				insertData(l);
			}
		}
	}
	
	public String[][] getSortedData() {
		List<Entry<String, Integer>> list = new LinkedList<Entry<String, Integer>>(this.allValues.entrySet());
		Collections.sort(list, new Comparator<Entry<String, Integer>>() {
			public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
				return o2.getValue().compareTo(o1.getValue());  
			}
		});
		
		Iterator<Entry<String, Integer>> it = list.iterator();
		
		for (int i = 0; i < this.sortedData.length; i++) {
			Entry<String, Integer> ent = it.next();
			this.sortedData[i][0] = ent.getKey();
			this.sortedData[i][1] = ent.getValue().toString();
		}
		
		/*Arrays.sort(this.sortedData, new Comparator<String[]>() {
			  @Override
			  public int compare(String[] i, String[] j) {
			    int x = Integer.parseInt(i[1]);
			    int y = Integer.parseInt(j[1]);
			    return x > y? 1 : 0;
			  }
			}.reversed());*/
		return this.sortedData;
	}
}
