package queries;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
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
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.tdb.TDBFactory;

import bgps.ExtractBgps;
import bgps.ManipulateBgps;
import cache.SolutionCache;
import common_joins.Joins;
import main.SingleQuery;
import utils.ArrayPermutations;

public class ExecuteQueries {
	
	public static void main(String[] args) throws Exception {
		// Example queries
		
		String q = "SELECT *\n "
				 + "WHERE {"
				 + "?s ?p ?o"
				 + "}\n"
				 + "LIMIT 100";
		
		String qPs = "SELECT DISTINCT ?p\n"
				 + "WHERE {"
				 + "?s ?p ?o"
				 + "}\n";
		
		// Read my TDB dataset
		String dbDir = "D:\\tmp\\DB10M";
		Dataset ds = TDBFactory.createDataset(dbDir);
		// Write if I wanna write, but I'll be using read to query over it mostly
		ds.begin(ReadWrite.READ);
		
		// Define model and Query
		Model model = ds.getDefaultModel();
		Query qu = QueryFactory.create(qPs);
		
		QueryExecution exec = QueryExecutionFactory.create(qu, model);
		ResultSet results = exec.execSelect();
		//System.out.println(ResultSetFormatter.asText(results));
		/*PrintWriter w = new PrintWriter(new FileWriter("D:\\tmp\\properties.txt"));
		w.print(ResultSetFormatter.asText(results));
		w.close();*/
		LinkedHashSet<String> ls = new LinkedHashSet<String>();
		
		// Skip first 12 properties of my dataset which are not PX
		for (int i = 1; i <= 12; i++) {
			results.next();
		}
		
		// Add only properties
		while (ls.size() != 10) {
			String s = results.next().get("?p").toString();
			String fs = s.substring(s.lastIndexOf("/") + 1);
			ls.add(fs);
		}
		
		// Test query, we're caching the results of this
		String cp = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>"
				+ "SELECT DISTINCT ?s ?o WHERE {"
				+ "?s wiki:P31 ?o"
				+ "}";
		
		// Create canonicalised query version of this and extract its bgps
		Query qcp = QueryFactory.create(cp);
		SingleQuery sqcp = new SingleQuery(qcp.toString(), true, true, false, true);
		qcp = QueryFactory.create(sqcp.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> qcpBgps = ExtractBgps.getBgps(Algebra.compile(qcp));
		
		// Cache pair bgp and resultset
		SolutionCache myCache = new SolutionCache();
		QueryExecution qcpExec = QueryExecutionFactory.create(qcp, model);
		ResultSet qcpResults = qcpExec.execSelect();
		myCache.cache(qcpBgps.get(0), qcpResults);
		
		// Test query two, we're caching the results of this size 2 bgp
		String tcp = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>"
				+ "SELECT DISTINCT * WHERE {"
				+ "?s wiki:P31 ?o."
				+ "?s wiki:P19 ?t."
				+ "}";
				
		// Create canonicalised query version of this and extract its bgps
		Query tqcp = QueryFactory.create(tcp);
		SingleQuery tsqcp = new SingleQuery(tqcp.toString(), true, true, false, true);
		tqcp = QueryFactory.create(tsqcp.getQuery(), Syntax.syntaxARQ);
		
		// Cache pair bgp and resultset
		ArrayList<OpBGP> tqcpBgps = ExtractBgps.getBgps(Algebra.compile(tqcp));
		QueryExecution tqcpExec = QueryExecutionFactory.create(tqcp, model);
		ResultSet tqcpResults = tqcpExec.execSelect();
		myCache.cache(tqcpBgps.get(0), tqcpResults);
		
		// Testing query, goal is to cache first triple and join with other bgps
		String jp = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
				+ "SELECT DISTINCT ?s WHERE {"
				+ "?s wiki:P31 ?o."
				+ "?s wiki:P3602 ?u."
				+ "?s wiki:P19 ?t."
				+ "}";
		
		Query qjp = QueryFactory.create(jp);
		
		QueryExecution qjpExec = QueryExecutionFactory.create(qjp, model);
		ResultSet qjpResults = qjpExec.execSelect();
		
		// Creates list with bgps in order from bigger to smaller
		ArrayList<OpBGP> qjpBgps = ExtractBgps.getSplitBgps(Algebra.compile(qjp));
		ArrayList<ArrayList<OpBGP>> qjpSubBgps = Joins.getSubBGPs(qjpBgps);
		ArrayList<ArrayList<OpBGP>> qjpSortedSubBgps = new ArrayList<ArrayList<OpBGP>>();
		
		for (int i = qjpSubBgps.size() - 1; i >= 0; i--) {
			qjpSortedSubBgps.add(qjpSubBgps.get(i));
		}
		
		qjpBgps = ExtractBgps.separateBGPs(qjpBgps);
		ArrayList<OpBGP> qjpOldBgps = qjpBgps;
		
		for (int i = qjpBgps.size() - 1; i >= 0; i--) {
			ArrayList<OpBGP> sl = new ArrayList<OpBGP>();
			sl.add(qjpBgps.get(i));
			qjpSortedSubBgps.add(sl);
		}
		
		ArrayList<Op> cachedBgps = new ArrayList<Op>();

		for (Op op : qjpBgps) {
			cachedBgps.add(op);
		}
		
		for (ArrayList<OpBGP> bgpList : qjpSortedSubBgps) {
			ArrayList<OpBGP> canonbgpList = ExtractBgps.separateBGPs(bgpList);
			//ArrayList<ArrayList<OpBGP>> perms = ArrayPermutations.generatePerm(canonbgpList);
			
			//for (ArrayList<OpBGP> cb : perms) {
			OpBGP bgp = ExtractBgps.unifyBGPs(canonbgpList);
			bgp = ExtractBgps.canonBGP(bgp);
			
			if (myCache.isBgpInCacheV2(bgp) && ManipulateBgps.checkIfInBgp(cachedBgps, bgpList)) {
				cachedBgps = myCache.retrieveCache(cachedBgps, bgp);
				ManipulateBgps.removeBgps(cachedBgps, bgpList);
				//break;
				//}
			}
		}
		
		System.out.println(cachedBgps);
		
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
		Op opjoin = Algebra.optimize(dist);
		//System.out.println(opjoin);
		
		// Create query
		QueryIterator qit = Algebra.exec(opjoin, model);
		List<String> vars = new ArrayList<String>();
		vars.add("v0");
		ResultSet qitset = ResultSetFactory.create(qit, vars);
		//System.out.println(ResultSetFormatter.asText(qitset));
	}
}
