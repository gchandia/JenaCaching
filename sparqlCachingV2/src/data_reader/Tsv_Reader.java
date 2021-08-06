package data_reader;

import java.io.File;

public class Tsv_Reader {
	
	public static void main(String[] args) throws Exception {
		
		
		System.out.println("Begin 1 of 15");
		TopNData list = new TopNData(20);
		list.retrieveTopN(new File("D:\\wikidata_logs\\results\\size_2"));
		for (String[] s : list.getSortedData()) {
			System.out.print(s[0] + "\t");
			System.out.println(s[1]);
		}
		Tsv_Writer.writeBgps(new File("D:\\wikidata_logs\\topData\\bgps_size_2.tsv"), list.getSortedData());
		
		/*System.out.println("Begin 2 of 15");
		list = new TopNData(10);
		list.retrieveTopN(new File("D:\\tmp\\size2"));
		for (String[] s : list.getSortedData()) {
			System.out.print(s[0] + "\t");
			System.out.println(s[1]);
		}
		Tsv_Writer.writeBgps(new File("D:\\tmp\\Top10Data\\Top10Size2.tsv"), list.getSortedData());
		
		System.out.println("Begin 3 of 15");
		list = new TopNData(10);
		list.retrieveTopN(new File("D:\\tmp\\size3"));
		for (String[] s : list.getSortedData()) {
			System.out.print(s[0] + "\t");
			System.out.println(s[1]);
		}
		Tsv_Writer.writeBgps(new File("D:\\tmp\\Top10Data\\Top10Size3.tsv"), list.getSortedData());
		
		System.out.println("Begin 4 of 15");
		list = new TopNData(10);
		list.retrieveTopN(new File("D:\\tmp\\size4"));
		for (String[] s : list.getSortedData()) {
			System.out.print(s[0] + "\t");
			System.out.println(s[1]);
		}
		Tsv_Writer.writeBgps(new File("D:\\tmp\\Top10Data\\Top10Size4.tsv"), list.getSortedData());
		
		System.out.println("Begin 5 of 15");
		list = new TopNData(10);
		list.retrieveTopN(new File("D:\\tmp\\size5"));
		for (String[] s : list.getSortedData()) {
			System.out.print(s[0] + "\t");
			System.out.println(s[1]);
		}
		Tsv_Writer.writeBgps(new File("D:\\tmp\\Top10Data\\Top10Size5.tsv"), list.getSortedData());
		
		System.out.println("Begin 6 of 15");
		list = new TopNData(10);
		list.retrieveTopN(new File("D:\\tmp\\size6"));
		for (String[] s : list.getSortedData()) {
			System.out.print(s[0] + "\t");
			System.out.println(s[1]);
		}
		Tsv_Writer.writeBgps(new File("D:\\tmp\\Top10Data\\Top10Size6.tsv"), list.getSortedData());
		
		System.out.println("Begin 7 of 15");
		list = new TopNData(10);
		list.retrieveTopN(new File("D:\\tmp\\size7"));
		for (String[] s : list.getSortedData()) {
			System.out.print(s[0] + "\t");
			System.out.println(s[1]);
		}
		Tsv_Writer.writeBgps(new File("D:\\tmp\\Top10Data\\Top10Size7.tsv"), list.getSortedData());
		
		System.out.println("Begin 8 of 15");
		list = new TopNData(10);
		list.retrieveTopN(new File("D:\\tmp\\size8"));
		for (String[] s : list.getSortedData()) {
			System.out.print(s[0] + "\t");
			System.out.println(s[1]);
		}
		Tsv_Writer.writeBgps(new File("D:\\tmp\\Top10Data\\Top10Size8.tsv"), list.getSortedData());
		
		System.out.println("Begin 9 of 15");
		list = new TopNData(10);
		list.retrieveTopN(new File("D:\\tmp\\size9"));
		for (String[] s : list.getSortedData()) {
			System.out.print(s[0] + "\t");
			System.out.println(s[1]);
		}
		Tsv_Writer.writeBgps(new File("D:\\tmp\\Top10Data\\Top10Size9.tsv"), list.getSortedData());
		
		System.out.println("Begin 10 of 15");
		list = new TopNData(10);
		list.retrieveTopN(new File("D:\\tmp\\size10"));
		for (String[] s : list.getSortedData()) {
			System.out.print(s[0] + "\t");
			System.out.println(s[1]);
		}
		Tsv_Writer.writeBgps(new File("D:\\tmp\\Top10Data\\Top10Size10.tsv"), list.getSortedData());
		
		System.out.println("Begin 11 of 15");
		list = new TopNData(10);
		list.retrieveTopN(new File("D:\\tmp\\size11"));
		for (String[] s : list.getSortedData()) {
			System.out.print(s[0] + "\t");
			System.out.println(s[1]);
		}
		Tsv_Writer.writeBgps(new File("D:\\tmp\\Top10Data\\Top10Size11.tsv"), list.getSortedData());
		
		System.out.println("Begin 12 of 15");
		list = new TopNData(10);
		list.retrieveTopN(new File("D:\\tmp\\size12"));
		for (String[] s : list.getSortedData()) {
			System.out.print(s[0] + "\t");
			System.out.println(s[1]);
		}
		Tsv_Writer.writeBgps(new File("D:\\tmp\\Top10Data\\Top10Size12.tsv"), list.getSortedData());
		
		System.out.println("Begin 13 of 15");
		list = new TopNData(10);
		list.retrieveTopN(new File("D:\\tmp\\size13"));
		for (String[] s : list.getSortedData()) {
			System.out.print(s[0] + "\t");
			System.out.println(s[1]);
		}
		Tsv_Writer.writeBgps(new File("D:\\tmp\\Top10Data\\Top10Size13.tsv"), list.getSortedData());
		
		System.out.println("Begin 14 of 15");
		list = new TopNData(10);
		list.retrieveTopN(new File("D:\\tmp\\size14"));
		for (String[] s : list.getSortedData()) {
			System.out.print(s[0] + "\t");
			System.out.println(s[1]);
		}
		Tsv_Writer.writeBgps(new File("D:\\tmp\\Top10Data\\Top10Size14.tsv"), list.getSortedData());
		
		System.out.println("Begin 15 of 15");
		list = new TopNData(10);
		list.retrieveTopN(new File("D:\\tmp\\size15"));
		for (String[] s : list.getSortedData()) {
			System.out.print(s[0] + "\t");
			System.out.println(s[1]);
		}
		Tsv_Writer.writeBgps(new File("D:\\tmp\\Top10Data\\Top10Size15.tsv"), list.getSortedData());*/
	}
}
