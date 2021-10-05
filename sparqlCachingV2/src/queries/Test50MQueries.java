package queries;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.tdb.TDBFactory;

import common_joins.Parser;
import main.java.cl.uchile.dcc.main.*;

public class Test50MQueries {
	public static void main(String[] args) throws Exception {
		BufferedReader tsv = new BufferedReader(
				 new InputStreamReader(
						 new FileInputStream(new File("D:\\tmp\\bgps\\J3.txt"))));
		
		// Read my TDB dataset
		String dbDir = "D:\\tmp\\DB50M";
		Dataset ds = TDBFactory.createDataset(dbDir);
		// Write if I wanna write, but I'll be using read to query over it mostly
		ds.begin(ReadWrite.READ);
		
		// Define model and Query
		Model model = ds.getDefaultModel();
		
		String line = tsv.readLine();
		String exline = "SELECT * WHERE { ?x <http://www.wikidata.org/prop/direct/P1366> ?v } LIMIT 1000";
		Parser parser = new Parser();
		Query q = parser.parseDbPedia(line);
		SingleQuery sq = new SingleQuery(q.toString(), true, true, false, true);
		Query sqq = QueryFactory.create(sq.getQuery(), Syntax.syntaxARQ);
		System.out.println(sq.getVarMap());
		
		//System.out.println(q);
		Query q2 = parser.parseDbPedia(exline);
		
		QueryExecution exec = QueryExecutionFactory.create(q, model);
		QueryExecution exec2 = QueryExecutionFactory.create(q2, model);
		ResultSet results = exec.execSelect();
		ResultSet results2 = exec2.execSelect();
		System.out.println(ResultSetFormatter.asText(results));
		//System.out.println(ResultSetFormatter.asText(results2));
		
	}
}
