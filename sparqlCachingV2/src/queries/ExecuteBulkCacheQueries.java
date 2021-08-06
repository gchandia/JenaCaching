package queries;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QueryParseException;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFactory;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpDistinct;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpLeftJoin;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.resultset.ResultSetException;
import org.apache.jena.sparql.resultset.ResultSetMem;
import org.apache.jena.tdb.TDBFactory;

import bgps.ExtractBgps;
import bgps.ExtractOps;
import bgps.ManipulateBgps;
import cache.SolutionCache;
import common_joins.Joins;
import common_joins.Parser;
import main.SingleQuery;
import utils.ArrayPermutations;

public class ExecuteBulkCacheQueries {
	static long totalTime = 0;
	static long queryNumber = 1;
	
	public static void main(String[] args) throws Exception {
		SolutionCache myCache = new SolutionCache();
		
		// Read my TDB dataset
		String dbDir = "D:\\tmp\\DB50M";
		Dataset ds = TDBFactory.createDataset(dbDir);
		// Write if I wanna write, but I'll be using read to query over it mostly
		ds.begin(ReadWrite.READ);
		
		// Define model and Query
		Model model = ds.getDefaultModel();
		
		/*String s1 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "SELECT ?s WHERE {\n"
				+ "?s wiki:P31 we:Q5.\n"
				+ "}";
		
		Query q1 = QueryFactory.create(s1);
		SingleQuery sq1 = new SingleQuery(q1.toString(), true, true, false, true);
		q1 = QueryFactory.create(sq1.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q1Bgps = ExtractBgps.getBgps(Algebra.compile(q1));
		QueryExecution q1Exec = QueryExecutionFactory.create(q1, model);
		ResultSet q1Results = q1Exec.execSelect();
		//System.out.println(ResultSetFormatter.asText(q1Results));
		myCache.cache(q1Bgps.get(0), q1Results);
		
		/*
		String s2 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "SELECT ?t WHERE {\n"
				+ "?s wiki:P238 ?o."
				+ "?s wiki:P31 ?t."
				+ "}";
		
		Query q2 = QueryFactory.create(s2);
		SingleQuery sq2 = new SingleQuery(q2.toString(), true, true, false, true);
		q2 = QueryFactory.create(sq2.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q2Bgps = ExtractBgps.getBgps(Algebra.compile(q2));
		QueryExecution q2Exec = QueryExecutionFactory.create(q2, model);
		ResultSet q2Results = q2Exec.execSelect();
		//System.out.println(ResultSetFormatter.asText(q2Results));
		myCache.cache(q2Bgps.get(0), q2Results);
		
		
		String s3 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "SELECT * WHERE {\n"
				+ "?s wiki:P345 ?o."
				+ "}";
		
		Query q3 = QueryFactory.create(s3);
		SingleQuery sq3 = new SingleQuery(q3.toString(), true, true, false, true);
		q3 = QueryFactory.create(sq3.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q3Bgps = ExtractBgps.getBgps(Algebra.compile(q3));
		QueryExecution q3Exec = QueryExecutionFactory.create(q3, model);
		ResultSet q3Results = q3Exec.execSelect();
		//System.out.println(ResultSetFormatter.asText(q3Results));
		myCache.cache(q3Bgps.get(0), q3Results);
		
		
		String s4 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "SELECT ?s WHERE {\n"
				+ "?s wiki:P18 ?o."
				+ "}";
		
		Query q4 = QueryFactory.create(s4);
		SingleQuery sq4 = new SingleQuery(q4.toString(), true, true, false, true);
		q4 = QueryFactory.create(sq4.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q4Bgps = ExtractBgps.getBgps(Algebra.compile(q4));
		QueryExecution q4Exec = QueryExecutionFactory.create(q4, model);
		ResultSet q4Results = q4Exec.execSelect();
		//System.out.println(ResultSetFormatter.asText(q4Results));
		myCache.cache(q4Bgps.get(0), q4Results);
		
		
		String s5 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "SELECT ?s WHERE {\n"
				+ "?s wiki:P569 ?o."
				+ "}";
		
		Query q5 = QueryFactory.create(s5);
		SingleQuery sq5 = new SingleQuery(q5.toString(), true, true, false, true);
		q5 = QueryFactory.create(sq5.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q5Bgps = ExtractBgps.getBgps(Algebra.compile(q5));
		QueryExecution q5Exec = QueryExecutionFactory.create(q5, model);
		ResultSet q5Results = q5Exec.execSelect();
		//System.out.println(ResultSetFormatter.asText(q5Results));
		myCache.cache(q5Bgps.get(0), q5Results);
		
		
		String s6 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n"
				+ "SELECT * WHERE {\n"
				+ "?s schema:about ?o."
				+ "?o wiki:P345 ?t."
				+ "}";
		
		Query q6 = QueryFactory.create(s6);
		SingleQuery sq6 = new SingleQuery(q6.toString(), true, true, false, true);
		q6 = QueryFactory.create(sq6.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q6Bgps = ExtractBgps.getBgps(Algebra.compile(q6));
		//System.out.println(q6Bgps);
		QueryExecution q6Exec = QueryExecutionFactory.create(q6, model);
		ResultSet q6Results = q6Exec.execSelect();
		//System.out.println(ResultSetFormatter.asText(q6Results));
		myCache.cache(q6Bgps.get(0), q6Results);
		
		
		String s7 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n"
				+ "SELECT * WHERE {\n"
				+ "?s schema:about ?o."
				+ "?o wiki:P856 ?t."
				+ "}";
		
		Query q7 = QueryFactory.create(s7);
		SingleQuery sq7 = new SingleQuery(q7.toString(), true, true, false, true);
		q7 = QueryFactory.create(sq7.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q7Bgps = ExtractBgps.getBgps(Algebra.compile(q7));
		//System.out.println(q7Bgps);
		QueryExecution q7Exec = QueryExecutionFactory.create(q7, model);
		ResultSet q7Results = q7Exec.execSelect();
		//System.out.println(ResultSetFormatter.asText(q7Results));
		myCache.cache(q7Bgps.get(0), q7Results);
		
		
		String s8 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n"
				+ "SELECT * WHERE {\n"
				+ "?s wiki:P106 ?o."
				+ "?s wiki:P27 ?t."
				+ "?s wiki:P569 ?u."
				+ "}";
		
		Query q8 = QueryFactory.create(s8);
		SingleQuery sq8 = new SingleQuery(q8.toString(), true, true, false, true);
		q8 = QueryFactory.create(sq8.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q8Bgps = ExtractBgps.getBgps(Algebra.compile(q8));
		//System.out.println(q8Bgps);
		QueryExecution q8Exec = QueryExecutionFactory.create(q8, model);
		ResultSet q8Results = q8Exec.execSelect();
		//System.out.println(ResultSetFormatter.asText(q8Results));
		myCache.cache(q8Bgps.get(0), q8Results);
		
		
		String s9 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n"
				+ "SELECT * WHERE {\n"
				+ "?s wiki:P106 ?o."
				+ "?s wiki:P18 ?t."
				+ "?s wiki:P569 ?u."
				+ "}";
		
		Query q9 = QueryFactory.create(s9);
		SingleQuery sq9 = new SingleQuery(q9.toString(), true, true, false, true);
		q9 = QueryFactory.create(sq9.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q9Bgps = ExtractBgps.getBgps(Algebra.compile(q9));
		//System.out.println(q9Bgps);
		QueryExecution q9Exec = QueryExecutionFactory.create(q9, model);
		ResultSet q9Results = q9Exec.execSelect();
		//System.out.println(ResultSetFormatter.asText(q9Results));
		myCache.cache(q9Bgps.get(0), q9Results);
		
		
		String s10 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n"
				+ "SELECT * WHERE {\n"
				+ "?s wiki:P106 ?o."
				+ "?s wiki:P18 ?t."
				+ "?s wiki:P27 ?u."
				+ "}";
		
		Query q10 = QueryFactory.create(s10);
		SingleQuery sq10 = new SingleQuery(q10.toString(), true, true, false, true);
		q10 = QueryFactory.create(sq10.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q10Bgps = ExtractBgps.getBgps(Algebra.compile(q10));
		//System.out.println(q10Bgps);
		QueryExecution q10Exec = QueryExecutionFactory.create(q10, model);
		ResultSet q10Results = q10Exec.execSelect();
		//System.out.println(ResultSetFormatter.asText(q10Results));
		myCache.cache(q10Bgps.get(0), q10Results);
		*/
		
		String s11 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n"
				+ "SELECT  *\n"
				+ "WHERE\n"
				+ "  { ?x  <http://www.wikidata.org/prop/direct/P1366> ?v\n"
				+ "  }";
		
		Query q11 = QueryFactory.create(s11);
		SingleQuery sq11 = new SingleQuery(q11.toString(), true, true, false, true);
		q11 = QueryFactory.create(sq11.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q11Bgps = ExtractBgps.getBgps(Algebra.compile(q11));
		//System.out.println(q10Bgps);
		QueryExecution q11Exec = QueryExecutionFactory.create(q11, model);
		ResultSet q11Results = q11Exec.execSelect();
		//ResultSetMem r = new ResultSetMem(q11Results);
		//System.out.println(r.size());
		//System.out.println(ResultSetFormatter.asText(q10Results));
		myCache.cache(q11Bgps.get(0), q11Results);
		
		
		String s12 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n"
				+ "SELECT  *\n"
				+ "WHERE\n"
				+ "  { ?x  <http://www.wikidata.org/prop/direct/P1269> ?v\n"
				+ "  }";
		
		Query q12 = QueryFactory.create(s12);
		SingleQuery sq12 = new SingleQuery(q12.toString(), true, true, false, true);
		q12 = QueryFactory.create(sq12.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q12Bgps = ExtractBgps.getBgps(Algebra.compile(q12));
		//System.out.println(q10Bgps);
		QueryExecution q12Exec = QueryExecutionFactory.create(q12, model);
		ResultSet q12Results = q12Exec.execSelect();
		//ResultSetMem r = new ResultSetMem(q11Results);
		//System.out.println(r.size());
		//System.out.println(ResultSetFormatter.asText(q10Results));
		myCache.cache(q12Bgps.get(0), q12Results);
		
		
		String s13 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n"
				+ "SELECT  *\n"
				+ "WHERE\n"
				+ "  { ?x  <http://www.wikidata.org/prop/direct/P1424> ?v\n"
				+ "  }";
		
		Query q13 = QueryFactory.create(s13);
		SingleQuery sq13 = new SingleQuery(q13.toString(), true, true, false, true);
		q13 = QueryFactory.create(sq13.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q13Bgps = ExtractBgps.getBgps(Algebra.compile(q13));
		//System.out.println(q10Bgps);
		QueryExecution q13Exec = QueryExecutionFactory.create(q13, model);
		ResultSet q13Results = q13Exec.execSelect();
		//ResultSetMem r = new ResultSetMem(q11Results);
		//System.out.println(r.size());
		//System.out.println(ResultSetFormatter.asText(q10Results));
		myCache.cache(q13Bgps.get(0), q13Results);
		
		
		String s14 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n"
				+ "SELECT  *\n"
				+ "WHERE\n"
				+ "{ ?x <http://www.wikidata.org/prop/direct/P1424> ?v .\n"
				+ "  ?v <http://www.wikidata.org/prop/direct/P1423> ?y\n"
				+ "  }";
		
		Query q14 = QueryFactory.create(s14);
		SingleQuery sq14 = new SingleQuery(q14.toString(), true, true, false, true);
		q14 = QueryFactory.create(sq14.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q14Bgps = ExtractBgps.getBgps(Algebra.compile(q14));
		//System.out.println(q10Bgps);
		QueryExecution q14Exec = QueryExecutionFactory.create(q14, model);
		ResultSet q14Results = q14Exec.execSelect();
		//ResultSetMem r = new ResultSetMem(q11Results);
		//System.out.println(r.size());
		//System.out.println(ResultSetFormatter.asText(q10Results));
		myCache.cache(q14Bgps.get(0), q14Results);
		
		BufferedReader tsv = new BufferedReader(
								 new InputStreamReader(
										 new FileInputStream(new File("D:\\tmp\\bgps\\J3.txt"))));
		
		// Write how much time each query takes
		PrintWriter w = new PrintWriter(new FileWriter("D:\\tmp\\bgps_logs_time\\CacheJ3Test.txt"));
		
		for (int i = 1; i <= 1; i++) {
			System.out.println("Executing query " + i + " of 1");
			final Runnable stuffToDo = new Thread() {
				@Override 
				public void run() {
					try {
						// Parse line and get query q
						String line = tsv.readLine();
						Parser parser = new Parser();
						w.println("Executing query " + queryNumber);
						long startLine = System.nanoTime();
						Query q = parser.parseDbPedia(line);
						long afterParse = System.nanoTime();
						w.println("Time to parse: " + (afterParse - startLine));
						
						// Get query bgps
						ArrayList<OpBGP> bgps = ExtractBgps.getSplitBgps(Algebra.compile(q));
						
						// Get all query subbgps of size two and more
						ArrayList<ArrayList<OpBGP>> subBgps = Joins.getSubBGPs(bgps);
						
						// We're sorting all subbgps from biggest to smallest here
						ArrayList<ArrayList<OpBGP>> sortedSubBgps = new ArrayList<ArrayList<OpBGP>>();
						
						// Add subbgps of size two and more
						for (int i = subBgps.size() - 1; i >= 0; i--) {
							sortedSubBgps.add(subBgps.get(i));
						}
						
						// Extract size one subBgps
						ArrayList<OpBGP> sizeOneBgps = ExtractBgps.separateBGPs(bgps);
						
						
						// Add size one subBgps to our sorted array
						for (int i = sizeOneBgps.size() - 1; i >= 0; i--) {
							ArrayList<OpBGP> sl = new ArrayList<OpBGP>();
							sl.add(sizeOneBgps.get(i));
							sortedSubBgps.add(sl);
						}
						
						// We add each Op instance in bgps to the array we'll work on with the cache
						ArrayList<Op> cachedBgps = new ArrayList<Op>();
						Op newAlg = null;
						ArrayList<OpBGP> sepBgps = ExtractBgps.separateBGPs(bgps);

						for (Op op : sepBgps) {
							cachedBgps.add(op);
						}
						
						long beforeCache = System.nanoTime();
						w.println("Time before registering cache is: " + (beforeCache - startLine));
						// For each subBgp, we check if they're in cache and if they haven't been found already, then we retrieve from our cache
						for (ArrayList<OpBGP> bgpList : sortedSubBgps) {
							ArrayList<OpBGP> canonbgpList = ExtractBgps.separateBGPs(bgpList);
							//ArrayList<ArrayList<OpBGP>> perms = ArrayPermutations.generatePerm(canonbgpList);
							
							//for (ArrayList<OpBGP> cb : perms) {
							OpBGP bgp = ExtractBgps.unifyBGPs(canonbgpList);
							Map<String, String> vars = new HashMap<String, String>();
							try {
								bgp = ExtractBgps.canonBGP(bgp);
								vars = ExtractBgps.getVarMap();
							} catch (Exception e) {}
								
							if (myCache.isBgpInCacheV2(bgp) && ManipulateBgps.checkIfInBgp(cachedBgps, bgpList)) {
								w.println("Query " + queryNumber + " ENTERED CACHE");
								long whenCache = System.nanoTime();
								w.println("Time before retrieving from cache: " + (whenCache - startLine));
								cachedBgps = myCache.retrieveCacheV2(cachedBgps, bgp, bgpList, ExtractBgps.getVarMap(), 0);
								newAlg = myCache.retrieveCacheV3(Algebra.compile(q), cachedBgps, bgp, bgpList, ExtractBgps.getVarMap());
								//ManipulateBgps.removeBgps(cachedBgps, bgpList);
									//break;
								//}
							}
						}
						
						//System.out.println(cachedBgps);
						System.out.println(Algebra.compile(q));
						System.out.println(newAlg);
						
						
						// Join Bgps
						Op join = null;
							
						if (cachedBgps.size() == 1) {
							join = cachedBgps.get(0);
						} else {
							join = OpJoin.create(cachedBgps.get(0), cachedBgps.get(1));
						}
						
						for (int i = 2; i < cachedBgps.size(); i++) {
							join = OpJoin.create(join, cachedBgps.get(i));
						}
						
						/*List<Var> projVars = new ArrayList<Var>();
						projVars.add(Var.alloc("v0"));
						projVars.add(Var.alloc("v1"));
						projVars.add(Var.alloc("v2"));
						projVars.add(Var.alloc("v3"));
						Op proj = new OpProject(join, projVars);*/
						Op dist = OpDistinct.create(join);
						
						// Get ops without bgps from query
						ArrayList<Op> queryOps = ExtractOps.getOps(Algebra.compile(q));
						System.out.println(queryOps);
						
						for (int i = queryOps.size() - 1; i >= 0; i--) {
							Op op = queryOps.get(i);
							if (op instanceof OpProject) {
								
							} else if (op instanceof OpLeftJoin) {
								
							}
						}
						
						// Optimize
						long beforeOptimize = System.nanoTime();
						w.println("Time before optimizing: " + (beforeOptimize - startLine));
						Op opjoin = Algebra.optimize(dist);
						
						//System.out.println(opjoin);
						
						// Create query
						long start = System.nanoTime();
						w.println("Time before reading results: " + (start - startLine));
						
						QueryIterator qit = Algebra.exec(opjoin, model);
						
						int resultAmount = 0;
						while (qit.hasNext()) {
							Binding b = qit.next();
							resultAmount++;
						}
						
						long stop = System.nanoTime();
						w.println("Time after reading all results: " + (stop - startLine));
						System.out.println(resultAmount);
						w.println("Amount of results is: " + resultAmount);
						
						/*List<String> vars = new ArrayList<String>();
						vars.add("x");
						vars.add("y");
						vars.add("z");
						vars.add("v");
						
						ResultSet qitset = ResultSetFactory.create(qit, vars);
						ResultSetMem r = new ResultSetMem(qitset);
						System.out.println(r.size());*/
						//System.out.println(ResultSetFormatter.asText(qitset));
						totalTime += (stop - start);
						System.out.println("Total time is: " + totalTime);
						w.println("Query number " + queryNumber++ + " takes " + (stop - startLine) + " nanoseconds\n");
						
					} catch (IllegalArgumentException e) {}
					catch (ResultSetException e) {}
					catch (QueryParseException e) {}
					catch (IOException e) {}
					catch (Exception e) {}
				}
			};

			final ExecutorService executor = Executors.newSingleThreadExecutor();
			@SuppressWarnings("rawtypes")
			final Future future = executor.submit(stuffToDo);
			executor.shutdown(); // This does not cancel the already-scheduled task.
			
			try { 
				  future.get(9999, TimeUnit.MINUTES);
			} catch (InterruptedException ie) {}
			catch (ExecutionException ee) {}
			catch (TimeoutException te) {}
			
			
		}
		w.close();
	}
}
