package data_reader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.zip.GZIPInputStream;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class JsonReader {
	
	public static JSONObject extractObject(String input) throws Exception {
		JSONParser jsonParser = new JSONParser();
		String line = input.substring(0, input.length() - 1);
    	Object obj = jsonParser.parse(line);
    	JSONObject lineList = (JSONObject) obj;
    	return lineList;
	}
	
	public static ArrayList<JSONObject> extractNObjects(File input, int N) {
		ArrayList<JSONObject> objs = new ArrayList<>();
		
		//JSON parser object to parse read file
        JSONParser jsonParser = new JSONParser();
        
        try (BufferedReader tsv = new BufferedReader (
        						  new InputStreamReader(
        						  new GZIPInputStream(
        						  new FileInputStream(input)))))
        {
        	tsv.readLine();
        	
        	for (int i = 0; i < N; i++) {
        		String line = tsv.readLine();
            	line = line.substring(0, line.length() - 1);
            	
            	Object obj = jsonParser.parse(line);
            	JSONObject lineList = (JSONObject) obj;
            	objs.add(lineList);
        	}
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
			e.printStackTrace();
		}
        
		return objs;
	}
	
	public static void parseJsonLine(JSONObject line) {
		
	}
	
	@SuppressWarnings("unchecked")
	public static void main(String[] args) 
    {
        //JSON parser object to parse read file
        JSONParser jsonParser = new JSONParser();
        
        try (BufferedReader tsv = new BufferedReader (
        						  new InputStreamReader(
        						  new GZIPInputStream(
        						  new FileInputStream(new File("D:\\Descargas\\latest-truthy.nt.gz"))))))
        {
        	/*tsv.readLine();
        	String lineOne = tsv.readLine();
        	lineOne = lineOne.substring(0, lineOne.length() - 1);
        	System.out.println(lineOne.substring(0, 500));
        	
        	Object obj = jsonParser.parse(lineOne);
        	JSONObject lineOneList = (JSONObject) obj;
        	
        	String id = (String) lineOneList.get("id");
        	System.out.println(id);*/
        	PrintWriter w = new PrintWriter(new FileWriter("D:\\Descargas\\wikidata-20210107-10m.nt"));
        	for (int i = 1; i <= 10000000; i++) {
        		String line = tsv.readLine();
        		w.println(line);
        		System.out.println(i);
        	}
        	w.close();
        	
        	//lineOneList.forEach( ent -> parseJsonLine( (JSONObject) ent ) );
        	
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } /*catch (ParseException e) {
			e.printStackTrace();
		}*/
    }
}
