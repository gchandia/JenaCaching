package queries;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryParseException;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFactory;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpDistinct;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.resultset.ResultSetException;
import org.apache.jena.sparql.resultset.ResultSetMem;
import org.apache.jena.tdb.TDBFactory;

import bgps.ExtractBgps;
import common_joins.Joins;
import common_joins.Parser;

public class ExecuteBulkQueries {
	static long totalTime = 0;
	static long queryNumber = 1;
	
	public static void main(String[] args) throws Exception {
		
		
		// Read my TDB dataset
		String dbDir = "D:\\tmp\\DB50M";
		Dataset ds = TDBFactory.createDataset(dbDir);
		// Write if I wanna write, but I'll be using read to query over it mostly
		ds.begin(ReadWrite.READ);
		
		// Define model and Query
		Model model = ds.getDefaultModel();
		
		/*List<String> result = new ArrayList<String>();
		tsv.readLine();
		String line = tsv.readLine();
		for (int i = 1; i <= 2000000; i++) {
			result.add(line);
			line = tsv.readLine();
		}
		
		Collections.shuffle(result);
		
		PrintWriter w = new PrintWriter(new FileWriter("D:\\tmp\\2MQueries.tsv"));
		for (String s : result) {
			w.println(s);
		}
		w.close();*/
		
		// 50K queries in 2 hours approx
		
		// Write how much time each query takes
		BufferedReader tsv = new BufferedReader(
				 new InputStreamReader(
						 		  		 new FileInputStream(new File("D:\\wikidata_logs\\Queries.tsv"))));
		PrintWriter w = new PrintWriter(new FileWriter("D:\\tmp\\NoCacheQueries.txt"));
		
		for (int i = 1; i <= 10000; i++) {
			System.out.println("Executing query " + i + " of 10000");
			final Runnable stuffToDo = new Thread() {
				@Override 
				public void run() { 
					try {
						String line = tsv.readLine();
						Parser parser = new Parser();
						w.println("Executing query " + queryNumber);
						long startLine = System.nanoTime();
						Query q = parser.parseDbPedia(line);
						long afterParse = System.nanoTime();
						w.println("Time to parse: " + (afterParse - startLine));
						//System.out.println(q);
						
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
						ArrayList<OpBGP> sepBgps = ExtractBgps.separateBGPs(bgps);

						for (Op op : sepBgps) {
							cachedBgps.add(op);
						}
						
						
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

						
						List<Var> projVars = new ArrayList<Var>();
						projVars.add(Var.alloc("v0"));
						Op proj = new OpProject(join, projVars);
						Op dist = OpDistinct.create(proj);
						
						// Optimize
						long beforeOptimize = System.nanoTime();
						w.println("Time before optimizing: " + (beforeOptimize - startLine));
						Op opjoin = Algebra.optimize(join);

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
						w.println("Amount of results for query " + queryNumber + " is: "  + resultAmount);
						
						/*List<String> vars = new ArrayList<String>();
						vars.add("x");
						vars.add("y");
						vars.add("z");
						vars.add("v");
						
						ResultSet qitset = ResultSetFactory.create(qit, vars);
						ResultSetMem r = new ResultSetMem(qitset);
						System.out.println(r.size());*/
						//System.out.println(ResultSetFormatter.asText(qitset));
						long time = stop - startLine;
						totalTime += time;
						System.out.println("Total time is: " + totalTime);
						w.println("Query number " + queryNumber++ + " takes " + time + " nanoseconds\n");
						
						/*QueryExecution exec = QueryExecutionFactory.create(q, model);
						long start = System.nanoTime();
						ResultSet results = exec.execSelect();
						long stop = System.nanoTime();
						//System.out.println(ResultSetFormatter.asText(results));
						totalTime += (stop - start);
						System.out.println("Total time is: " + totalTime);
						w.println("Query number " + queryNumber++ + " takes " + (stop - start) + " nanoseconds");*/
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
				  future.get(1, TimeUnit.MINUTES); 
			} catch (InterruptedException ie) {}
			catch (ExecutionException ee) {}
			catch (TimeoutException te) {}
		}
		
		w.close();
	}
}
