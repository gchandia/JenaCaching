package bgps;

import java.util.ArrayList;
import java.util.Map;

import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.Syntax;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.Op1;
import org.apache.jena.sparql.algebra.op.Op2;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpN;
import org.apache.jena.sparql.algebra.op.OpPath;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.syntax.ElementGroup;

import common_joins.Joins;
import common_joins.Parser;
import main.java.cl.uchile.dcc.main.SingleQuery;

public class ExtractBgps {
	
	private static Map<String, String> varMap;
	
	public static Map<String, String> getVarMap() {
		return varMap;
	}
	
	public static ArrayList<OpBGP> getBgps(Op op){
		ArrayList<OpBGP> bgps = new ArrayList<OpBGP>();
		getBgps(op,bgps);
		return bgps;
	}
	
	public static ArrayList<OpBGP> getSplitBgps(Op op){
		ArrayList<OpBGP> splitBgps = new ArrayList<OpBGP>();
		getSplitBgps(op,splitBgps);
		return splitBgps;
	}
	
	public static void getBgps(Op op, ArrayList<OpBGP> bgps){
		if(op instanceof OpBGP) {
			bgps.add((OpBGP)op);
		}else if (op instanceof OpPath) {
			TriplePath path = ((OpPath)op).getTriplePath();
			BasicPattern bp = new BasicPattern();
			Triple nt = Triple.create(path.getSubject(), NodeFactory.createURI(path.getPath().toString()), path.getObject());
			bp.add(nt);
			bgps.add(new OpBGP(bp));
		} else if(op instanceof Op1) {
			getBgps(((Op1)op).getSubOp(),bgps);
		} else if(op instanceof Op2) {
			getBgps(((Op2)op).getLeft(),bgps);
			getBgps(((Op2)op).getRight(),bgps);
		} else if(op instanceof OpN) {
			OpN opn = (OpN) op;
			for(Op sop:opn.getElements()) {
				getBgps(sop,bgps);
			}
		}
	}
	
	/**
	 * WILL COUNT PROPERTY PATHS AS A NORMAL TRIPLE
	 * @param op
	 * @param splitbgps
	 */
	public static void getSplitBgps(Op op, ArrayList<OpBGP> splitbgps) {
		if (op instanceof OpBGP) {
			ArrayList<OpBGP> l = new ArrayList<OpBGP>();
			splitbgps.add((OpBGP)op);
		} else if (op instanceof OpPath) {
			ArrayList<OpBGP> l = new ArrayList<OpBGP>();
			TriplePath path = ((OpPath)op).getTriplePath();
			BasicPattern bp = new BasicPattern();
			Triple nt = Triple.create(path.getSubject(), NodeFactory.createURI(path.getPath().toString()), path.getObject());
			bp.add(nt);
			splitbgps.add(new OpBGP(bp));
		} else if (op instanceof Op1) {
			getSplitBgps(((Op1)op).getSubOp(), splitbgps);
		} else if (op instanceof Op2) {
			getSplitBgps(((Op2)op).getLeft(), splitbgps);
			getSplitBgps(((Op2)op).getRight(), splitbgps);
		} else if(op instanceof OpN) {
			OpN opn = (OpN) op;
			for(Op sop : opn.getElements()) {
				getSplitBgps(sop, splitbgps);
			}
		}
	}
	
	public static void extractBGP(Query q) {
		Op op = Algebra.compile(q);
		ArrayList<OpBGP> opbgps = getBgps(op);
		System.out.println(opbgps.toString());
	}
	
	public static void extractSplitBGPs(Query q) {
		Op op = Algebra.compile(q);
		ArrayList<OpBGP> splitbgps = getSplitBgps(op);
		System.out.println(splitbgps.toString());
	}
	
	public static OpBGP unifyBGPs(ArrayList<OpBGP> input) {
		BasicPattern bp = new BasicPattern();
		
		for (OpBGP bgp : input) {
			bp.add(bgp.getPattern().get(0));
		}
		
		OpBGP output = new OpBGP(bp);
		return output;
	}
	
	public static OpBGP canonBGP(OpBGP input) throws Exception {
		Query q = QueryFactory.make();
		q.setQuerySelectType();
		q.setQueryResultStar(true);
		ElementGroup elg = new ElementGroup();
		for (int i = 0; i < input.getPattern().size(); i++) {
			elg.addTriplePattern(input.getPattern().get(i));
		}
		q.setQueryPattern(elg);
		SingleQuery sq = new SingleQuery(q.toString(), true, true, false, true);
		q = QueryFactory.create(sq.getQuery(), Syntax.syntaxARQ);
		Op op = Algebra.compile(q);
		ArrayList<OpBGP> bgps = getBgps(op);
		varMap = sq.getVarMap();
		return bgps.get(0);
	}
	
	/**
	 * Only separates bgps into chunks of size 1
	 * @param input
	 * @return
	 * @throws Exception
	 */
	public static ArrayList<OpBGP> separateBGPs(ArrayList<OpBGP> input) {
		ArrayList<OpBGP> output = new ArrayList<OpBGP>();
		
		for (int i = 0; i < input.size(); i++) {
			OpBGP bgp = input.get(i);
			for (int j = 0; j < bgp.getPattern().size(); j++) {
				Triple t = input.get(i).getPattern().get(j);
				Query q = QueryFactory.make();
				q.setQuerySelectType();
				q.setQueryResultStar(true);
				ElementGroup elg = new ElementGroup();
				elg.addTriplePattern(t);
				q.setQueryPattern(elg);
				output.addAll(getBgps(Algebra.compile(q)));
			}
		}
		
		return output;
	}
	
	/*
	 * Gets bgps and returns bgps of size one that have been canonicalised
	 */
	public static ArrayList<OpBGP> separateCanonBGPs(ArrayList<OpBGP> input) throws Exception {
		ArrayList<OpBGP> output = new ArrayList<OpBGP>();
		
		for (int i = 0; i < input.size(); i++) {
			OpBGP bgp = input.get(i);
			for (int j = 0; j < bgp.getPattern().size(); j++) {
				Triple t = input.get(i).getPattern().get(j);
				Query q = QueryFactory.make();
				q.setQuerySelectType();
				q.setQueryResultStar(true);
				ElementGroup elg = new ElementGroup();
				elg.addTriplePattern(t);
				q.setQueryPattern(elg);
				SingleQuery sq = new SingleQuery(q.toString(), true, true, false, true);
				q = QueryFactory.create(sq.getQuery(), Syntax.syntaxARQ);
				output.addAll(getBgps(Algebra.compile(q)));
			}
		}
		
		return output;
	}
	
	public static void main(String[] args) throws Exception {
		String query = "SELECT ?x WHERE { ?x rdf:university ?y ."
						+ " ?y rdf:department/rdf:worksFor* ?c ."
						+ "?c rdf:mall ?d . "
						+ "}";
		String query2 = "SELECT  *"
						+ "WHERE"
						+ "{ ?v5  <http://dbpedia.org/ontology/abstract>  ?v3 ;"
						+ "<http://www.opengis.net/ont/geosparql#lat>  ?v1 ;"
				        + "a                     <http://dbpedia.org/ontology/PopulatedPlace> ;"
				        + "<http://www.w3.org/2000/01/rdf-schema#label>  ?var1 ;"
				        + "<http://www.w3.org/2000/01/rdf-schema#label>  ?v9;"
				  		+ "}";
		String query3 = "SELECT *\n"
						+ "WHERE {\n"
						+ "   ?w (rdf:p/rdf:q)* ?x .\n"
						+ "   ?x (rdf:r|rdf:s)* ?y .\n"
						+ "   ?y rdf:t ?z .\n"
						+ "}";
		String query4 = "SELECT *\n"
						+ "WHERE {\n"
						+ "   ?w (rdf:p/rdf:q)* ?x .\n"
						+ "   ?z ^((rdf:r|rdf:s)*/rdf:t) ?x .\n"
						+ "}";
		String query5 = "SELECT *\n"
						+ "WHERE {\n"
						+ "   ?w (rdf:t*/rdf:t*)* ?x .\n"
						+ "}";
		String query6 = "SELECT  ?v0 ?v1 ?v2 ?v3 ?v4\n"
						+ "WHERE\n"
						+ "  {   {   {   { ?v0  <http://dbpedia.org/ontology/iataLocationIdentifier>  ?v4 ;\n"
						+ "                     <http://dbpedia.org/ontology/location>  ?v2 ;\n"
						+ "                     a                     <http://dbpedia.org/ontology/Airport> .\n"
						+ "                ?v2  a                     <http://dbpedia.org/ontology/Settlement> ;\n"
						+ "                     <http://www.w3.org/2000/01/rdf-schema#label>  \"Crailsheim@de\"\n"
						+ "              }\n"
						+ "            UNION\n"
						+ "              { ?v0  <http://dbpedia.org/ontology/city>  ?v2 ;\n"
						+ "                     <http://dbpedia.org/property/iata>  ?v4 ;\n"
						+ "                     a                     <http://dbpedia.org/ontology/Airport> .\n"
						+ "                ?v2  a                     <http://dbpedia.org/ontology/Settlement> ;\n"
						+ "                     <http://www.w3.org/2000/01/rdf-schema#label>  \"Crailsheim@de\"\n"
						+ "              }\n"
						+ "          }\n"
						+ "        UNION\n"
						+ "          { ?v0  <http://dbpedia.org/ontology/city>  ?v2 ;\n"
						+ "                 <http://dbpedia.org/ontology/iataLocationIdentifier>  ?v4 ;\n"
						+ "                 a                     <http://dbpedia.org/ontology/Airport> .\n"
						+ "            ?v2  a                     <http://dbpedia.org/ontology/Settlement> ;\n"
						+ "                 <http://www.w3.org/2000/01/rdf-schema#label>  \"Crailsheim@de\"\n"
						+ "          }\n"
						+ "      }\n"
						+ "    UNION\n"
						+ "      { ?v0  <http://dbpedia.org/ontology/location>  ?v2 ;\n"
						+ "             <http://dbpedia.org/property/iata>  ?v4 ;\n"
						+ "             a                     <http://dbpedia.org/ontology/Airport> .\n"
						+ "        ?v2  a                     <http://dbpedia.org/ontology/Settlement> ;\n"
						+ "             <http://www.w3.org/2000/01/rdf-schema#label>  \"Crailsheim@de\"\n"
						+ "      }\n"
						+ "    OPTIONAL\n"
						+ "      { ?v0  <http://xmlns.com/foaf/0.1/homepage>  ?v1 }\n"
						+ "    OPTIONAL\n"
						+ "      { ?v0  <http://dbpedia.org/property/nativename>  ?v3 }\n"
						+ "  }";
		String query7 = "SELECT DISTINCT  ?v0 ?v1 ?v2 ?v3 ?v4 ?v5 ?v6 ?v7 ?v8 ?v9 ?v10 ?v11 ?v12 ?v13 ?v14\n"
						+ "WHERE\n"
						+ "  { ?v11  <http://dbpedia.org/ontology/assets>  ?v13 ;\n"
						+ "          <http://dbpedia.org/ontology/equity>  ?v3 ;\n"
						+ "          <http://dbpedia.org/ontology/foundationPlace>  ?v9 ;\n"
						+ "          <http://dbpedia.org/ontology/industry>  ?v1 ;\n"
						+ "          <http://dbpedia.org/ontology/location>  ?v12 ;\n"
						+ "          <http://dbpedia.org/ontology/netIncome>  ?v10 ;\n"
						+ "          <http://dbpedia.org/ontology/numberOfStaff>  ?v14 ;\n"
						+ "          <http://dbpedia.org/ontology/product>  ?v7 ;\n"
						+ "          <http://dbpedia.org/ontology/reference>  ?v8 ;\n"
						+ "          <http://dbpedia.org/ontology/revenue>  ?v6 ;\n"
						+ "          <http://dbpedia.org/property/numEmployees>  ?v4 ;\n"
						+ "          a                     <http://dbpedia.org/class/yago/Company108058098> ;\n"
						+ "          <http://www.w3.org/2000/01/rdf-schema#comment>  ?v0 ;\n"
						+ "          <http://www.w3.org/2000/01/rdf-schema#label>  ?v2 ;\n"
						+ "          <http://xmlns.com/foaf/0.1/homepage>  ?v5\n"
						+ "    FILTER ( ( ( lang(?v0) = \"en\" ) && ( lang(?v2) = \"en\" ) ) && ( <http://www.w3.org/2001/XMLSchema#integer>(?v4) >= 5000 ) )\n"
						+ "  }";
		String query8 = "SELECT  ?v0 ?v1 ?v2 ?v3 ?v4 ?v5\n"
						+ "WHERE\n"
						+ "  { {   {   {   {   {   {   {   { ?v0  a                     <http://dbpedia.org/ontology/Place> ;\n"
						+ "                                       <http://www.w3.org/2000/01/rdf-schema#label>  \"Fljótsdalshérað@da\" .\n"
						+ "                                  ?v5  <http://dbpedia.org/ontology/iataLocationIdentifier>  ?v4 ;\n"
						+ "                                       <http://dbpedia.org/ontology/location>  ?v0 ;\n"
						+ "                                       a                     <http://dbpedia.org/ontology/Airport>\n"
						+ "                                }\n"
						+ "                              UNION\n"
						+ "                                { ?v0  a                     <http://dbpedia.org/ontology/Place> ;\n"
						+ "                                       <http://www.w3.org/2000/01/rdf-schema#label>  \"Fljótsdalshérað@da\" .\n"
						+ "                                  ?v5  <http://dbpedia.org/property/cityServed>  ?v0 ;\n"
						+ "                                       <http://dbpedia.org/property/iata>  ?v4 ;\n"
						+ "                                       a                     <http://dbpedia.org/ontology/Airport>\n"
						+ "                                }\n"
						+ "                            }\n"
						+ "                          UNION\n"
						+ "                            { ?v0  a                     <http://dbpedia.org/ontology/Place> ;\n"
						+ "                                   <http://www.w3.org/2000/01/rdf-schema#label>  \"Fljótsdalshérað@da\" .\n"
						+ "                              ?v5  <http://dbpedia.org/ontology/city>  ?v0 ;\n"
						+ "                                   <http://dbpedia.org/property/iata>  ?v4 ;\n"
						+ "                                   a                     <http://dbpedia.org/ontology/Airport>\n"
						+ "                            }\n"
						+ "                        }\n"
						+ "                      UNION\n"
						+ "                        { ?v0  a                     <http://dbpedia.org/ontology/Place> ;\n"
						+ "                               <http://www.w3.org/2000/01/rdf-schema#label>  \"Fljótsdalshérað@da\" .\n"
						+ "                          ?v5  <http://dbpedia.org/ontology/city>  ?v0 ;\n"
						+ "                               <http://dbpedia.org/ontology/iataLocationIdentifier>  ?v4 ;\n"
						+ "                               a                     <http://dbpedia.org/ontology/Airport>\n"
						+ "                        }\n"
						+ "                    }\n"
						+ "                  UNION\n"
						+ "                    { ?v0  a                     <http://dbpedia.org/ontology/Place> ;\n"
						+ "                           <http://www.w3.org/2000/01/rdf-schema#label>  \"Fljótsdalshérað@da\" .\n"
						+ "                      ?v5  <http://dbpedia.org/ontology/city>  ?v0 ;\n"
						+ "                           <http://dbpedia.org/ontology/iataLocationIdentifier>  ?v4 ;\n"
						+ "                           a                     <http://dbpedia.org/ontology/Airport>\n"
						+ "                    }\n"
						+ "                }\n"
						+ "              UNION\n"
						+ "                { ?v0  a                     <http://dbpedia.org/ontology/Place> ;\n"
						+ "                       <http://www.w3.org/2000/01/rdf-schema#label>  \"Fljótsdalshérað@da\" .\n"
						+ "                  ?v5  <http://dbpedia.org/ontology/city>  ?v0 ;\n"
						+ "                       <http://dbpedia.org/property/iata>  ?v4 ;\n"
						+ "                       a                     <http://dbpedia.org/ontology/Airport>\n"
						+ "                }\n"
						+ "            }\n"
						+ "          UNION\n"
						+ "            { ?v0  a                     <http://dbpedia.org/ontology/Place> ;\n"
						+ "                   <http://www.w3.org/2000/01/rdf-schema#label>  \"Fljótsdalshérað@da\" .\n"
						+ "              ?v5  <http://dbpedia.org/ontology/location>  ?v0 ;\n"
						+ "                   <http://dbpedia.org/property/iata>  ?v4 ;\n"
						+ "                   a                     <http://dbpedia.org/ontology/Airport>\n"
						+ "            }\n"
						+ "        }\n"
						+ "      UNION\n"
						+ "        { ?v0  a                     <http://dbpedia.org/ontology/Place> ;\n"
						+ "               <http://www.w3.org/2000/01/rdf-schema#label>  \"Fljótsdalshérað@da\" .\n"
						+ "          ?v5  <http://dbpedia.org/ontology/iataLocationIdentifier>  ?v4 ;\n"
						+ "               <http://dbpedia.org/property/cityServed>  ?v0 ;\n"
						+ "               a                     <http://dbpedia.org/ontology/Airport>\n"
						+ "        }\n"
						+ "      OPTIONAL\n"
						+ "        { ?v5  <http://xmlns.com/foaf/0.1/homepage>  ?v1 }\n"
						+ "      OPTIONAL\n"
						+ "        { ?v5  <http://www.w3.org/2000/01/rdf-schema#label>  ?v2 }\n"
						+ "      OPTIONAL\n"
						+ "        { ?v5  <http://dbpedia.org/property/nativename>  ?v3 }\n"
						+ "    }\n"
						+ "    FILTER ( ( ! bound(?v2) ) || langMatches(lang(?v2), \"da\") )\n"
						+ "  }";
		String query9 = "SELECT DISTINCT ?x ?y ?z\n"
				+ "WHERE { ?x ?y ?z . VALUES ?s { \"s\" } }";
		Parser p = new Parser();
		Query q1 = p.parseDbPedia(query);
		Query q2 = p.parseDbPedia(query2);
		Query q3 = p.parseDbPedia(query3);
		Query q4 = p.parseDbPedia(query4);
		Query q5 = p.parseDbPedia(query5);
		Query q6 = p.parseDbPedia(query6);
		Query q7 = p.parseDbPedia(query7);
		Query q8 = p.parseDbPedia(query8);
		Query q9 = p.parseDbPedia(query9); //OpTable
		Op opq1 = Algebra.compile(q1);
		Op opq2 = Algebra.compile(q2);
		Op opq3 = Algebra.compile(q3);
		Op opq4 = Algebra.compile(q4);
		Op opq5 = Algebra.compile(q5);
		Op opq6 = Algebra.compile(q6);
		Op opq7 = Algebra.compile(q7);
		Op opq8 = Algebra.compile(q8);
		Op opq9 = Algebra.compile(q9);

		System.out.println(opq2);
		
		
		/*SingleQuery sq3 = new SingleQuery(q3.toString(), true, true, false, true);
		q3 = QueryFactory.create(sq3.getQuery(), Syntax.syntaxARQ);
		System.out.println(q3.toString());
		
		ArrayList<ArrayList<OpBGP>> q3bgps = getSplitBgps(Algebra.compile(q3));
		ArrayList<OpBGP> q3bgpsSimp = ManipulateBgps.collapseBgps(q3bgps);
		q3bgps = new ArrayList<ArrayList<OpBGP>>();
		q3bgps.add(q3bgpsSimp);
		
		System.out.println(q3bgpsSimp);

		/*Op op2 = ((Op1)op).getSubOp();
		for(Op sop : ((OpN)op2).getElements()) {
			if (sop instanceof OpPath) {
				System.out.println(sop);
				TriplePath path = ((OpPath)sop).getTriplePath(); // Predicate counts as null
				System.out.println(path.getSubject());
				System.out.println(path.getPath());
				System.out.println(path.getObject());
			}
			
		}
		extractBGP(q);*/
	}
}
