package transform;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpPath;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.TriplePath;

import bgps.ExtractBgps;
import bgps.ManipulateBgps;
import cache.SolutionCache;
import common_joins.Joins;

public class CacheTransformCopy extends TransformCopy {
	private SolutionCache myCache;
	private long startLine = 0;
	private String solution = "";
	
	public CacheTransformCopy(SolutionCache cache) {
		this.myCache = cache;
	}
	
	public CacheTransformCopy(SolutionCache cache, long startLine) {
		this.myCache = cache;
		this.startLine = startLine;
	}
	
	public String getSolution() {
		return this.solution;
	}
	
	public void formSolution(String input) {
		this.solution = input;
	}
	
	public Op transform(OpBGP bgp) {
		// Get query bgps
		ArrayList<OpBGP> bgps = ExtractBgps.getSplitBgps(bgp);

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
		
		long beforeCache = System.nanoTime();
		String bc = "Time before registering cache is: " + (beforeCache - startLine);
		String ec = "DIDN'T ENTER";
		String br = "DIDN'T RETRIEVE";
		String ar = "DIDN'T RETRIEVE";
		String sol = "DIDN'T RETRIEVE";
		
		for (ArrayList<OpBGP> bgpList : sortedSubBgps) {
			ArrayList<OpBGP> canonbgpList = ExtractBgps.separateBGPs(bgpList);
			//ArrayList<ArrayList<OpBGP>> perms = ArrayPermutations.generatePerm(canonbgpList);
			
			//for (ArrayList<OpBGP> cb : perms) {
			bgp = ExtractBgps.unifyBGPs(canonbgpList);
			Map<String, String> vars = new HashMap<String, String>();
			try {
				bgp = ExtractBgps.canonBGP(bgp);
				vars = ExtractBgps.getVarMap();
			} catch (Exception e) {}
			
			if (myCache.isBgpInCacheV2(bgp) && ManipulateBgps.checkIfInBgp(cachedBgps, bgpList)) {
				ec = "ENTERED CACHE";
				long whenCache = System.nanoTime();
				br = "Time before retrieving from cache: " + (whenCache - startLine);
				cachedBgps = myCache.retrieveCacheV2(cachedBgps, bgp, bgpList, vars, startLine);
				sol = myCache.getSolution();
				long afterCache = System.nanoTime();
				ar = "Time after retrieving from cache: " + (afterCache - startLine);
			}
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
		
		Op opjoin = Algebra.optimize(join);
		
		formSolution(bc + '\n' + ec + '\n' + br + '\n' + sol + '\n' + ar);
		
		return opjoin;
	}
	
	/*public Op transform (OpPath op) {
		TriplePath path = ((OpPath)op).getTriplePath();
		BasicPattern bp = new BasicPattern();
		Triple nt = Triple.create(path.getSubject(), NodeFactory.createURI(path.getPath().toString()), path.getObject());
		bp.add(nt);
		
		
		return transform(new OpBGP(bp));
	}*/
}
