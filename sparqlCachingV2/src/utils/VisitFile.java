package utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;

public class VisitFile {
	
	private static int countTotal(File entry) throws Exception {
		int total = 0;
		BufferedReader tsv = new BufferedReader(new InputStreamReader(new FileInputStream(entry)));
		while (true) {
			String line = tsv.readLine();
			if (line == null) break;
			total++;
		}
		tsv.close();
		return total;
	}
	
	public static void readFirstNLines(File entry, int n) throws Exception {
		BufferedReader tsv = new BufferedReader(new InputStreamReader(new FileInputStream(entry)));
		
		for (int i = 0; i < n; i++) {
			String line = tsv.readLine();
			if (line == null) break;
			System.out.println(line);
		}
		
		tsv.close();
	}
	
	public static void readLastNLines(File entry, int n) throws Exception {
		int readLimit = countTotal(entry) - n;
		BufferedReader tsv = new BufferedReader(new InputStreamReader(new FileInputStream(entry)));
		for (int i = 0; i < readLimit; i++) tsv.readLine();
		for (int i = 0; i < n; i++) {
			String line = tsv.readLine();
			if (line == null) break;
			System.out.println(line);
		}
		tsv.close();
	}
}
