package queries;

import java.util.ArrayList;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.tdb.TDBFactory;

import bgps.ExtractBgps;
import bgps.ManipulateBgps;
import cache.SolutionCache;
import cl.uchile.dcc.blabel.label.GraphColouring.HashCollisionException;
import common_joins.Joins;
import main.SingleQuery;
import utils.ArrayPermutations;

public class ExecuteCacheQueries {
	
	public static void executeQuery(Query q, SolutionCache myCache) {
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
		
		// For each subBgp, we check if they're in cache and if they haven't been found already, then we retrieve from our cache
		for (ArrayList<OpBGP> bgpList : sortedSubBgps) {
			ArrayList<OpBGP> canonbgpList = ExtractBgps.separateBGPs(bgpList);
			ArrayList<ArrayList<OpBGP>> perms = ArrayPermutations.generatePerm(canonbgpList);
			
			for (ArrayList<OpBGP> cb : perms) {
				OpBGP bgp = ExtractBgps.unifyBGPs(cb);
				try {
					bgp = ExtractBgps.canonBGP(bgp);
				} catch (Exception e) {
					e.printStackTrace();
				}
				
				if (myCache.isBgpInCache(bgp) && ManipulateBgps.checkIfInBgp(cachedBgps, bgpList)) {
					cachedBgps = myCache.retrieveCache(cachedBgps, bgp);
					ManipulateBgps.removeBgps(cachedBgps, bgpList);
					break;
				}
			}
		}
		
		System.out.println(cachedBgps);
	}
	
	public static void main(String[] args) {
		// Our in memory Cache
		SolutionCache myCache = new SolutionCache();
		
		// Read my TDB dataset
		String dbDir = "D:\\tmp\\DB10M";
		Dataset ds = TDBFactory.createDataset(dbDir);
		// Write if I wanna write, but I'll be using read to query over it mostly
		ds.begin(ReadWrite.READ);
				
		// Define model and Query
		Model model = ds.getDefaultModel();
		
		// Test queries, first one we cache, second one we execute
		String tcp = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>"
				+ "SELECT DISTINCT ?s ?o WHERE {"
				+ "?s wiki:P31 ?o."
				+ "}";
		
		String jp = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
				+ "SELECT DISTINCT ?s WHERE {"
				+ "?s wiki:P31 ?o."
				+ "?s wiki:P19 ?t."
				+ "?s wiki:P3602 ?u"
				+ "}";
		
		// Create query for our caching query
		Query cq = QueryFactory.create(tcp);
		
		// We canonicalise our query to cache
		SingleQuery scq = null;
		try {
			scq = new SingleQuery(cq.toString(), true, true, false, true);
		} catch (HashCollisionException e) {}
		catch (InterruptedException e) {}
		
		cq = QueryFactory.create(scq.getQuery(), Syntax.syntaxARQ);
		
		// We extract the canonicalised query's bgp so we save it in the cache
		ArrayList<OpBGP> cqBgps = ExtractBgps.getBgps(Algebra.compile(cq));
		
		// Obtain the result set of the query we want to cache
		QueryExecution cqExec = QueryExecutionFactory.create(cq, model);
		ResultSet cqResults = cqExec.execSelect();
		
		// Cache pair bgp-resultSet
		myCache.cache(cqBgps.get(0), cqResults);
		
		Query q = QueryFactory.create(jp);
		executeQuery(q, myCache);
	}

}
