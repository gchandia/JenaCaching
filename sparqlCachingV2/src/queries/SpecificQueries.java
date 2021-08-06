package queries;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Transform;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.tdb.TDBFactory;

import cache.FillCache;
import transform.CacheTransformCopy;

public class SpecificQueries {
	
	static String s1 = "PREFIX  owl:  <http://www.w3.org/2002/07/owl#>\r\n"
			+ "PREFIX  yago: <http://www.w3.org/2000/01/rdf-schema#>\r\n"
			+ "PREFIX  xsd:  <http://www.w3.org/2001/XMLSchema#>\r\n"
			+ "PREFIX  skos: <http://www.w3.org/2004/02/skos/core#>\r\n"
			+ "PREFIX  rdfs: <http://www.w3.org/2000/01/rdf-schema#>\r\n"
			+ "PREFIX  type: <http://info.deepcarbon.net/schema/type#>\r\n"
			+ "PREFIX  ub:   <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>\r\n"
			+ "PREFIX  sioct: <http://rdfs.org/sioc/types#>\r\n"
			+ "PREFIX  geo:  <http://www.opengis.net/ont/geosparql#>\r\n"
			+ "PREFIX  dbpo: <http://dbpedia.org/ontology/>\r\n"
			+ "PREFIX  prop: <http://dbpedia.org/property/>\r\n"
			+ "PREFIX  dbpedia2: <http://dbpedia.org/property/>\r\n"
			+ "PREFIX  dbpprop: <http://dbpedia.org/property/>\r\n"
			+ "PREFIX  foaf: <http://xmlns.com/foaf/0.1/>\r\n"
			+ "PREFIX  dbprop: <http://dbpedia.org/property/>\r\n"
			+ "PREFIX  sioc: <http://rdfs.org/sioc/ns#>\r\n"
			+ "PREFIX  mo:   <http://purl.org/ontology/mo/>\r\n"
			+ "PREFIX  abc:  <http://www.metadata.net/harmony/ABCSchemaV5Commented.rdf#>\r\n"
			+ "PREFIX  dbpedia-owl: <http://dbpedia.org/ontology/>\r\n"
			+ "PREFIX  gn:   <http://www.geonames.org/ontology#>\r\n"
			+ "PREFIX  dbpedia: <http://dbpedia.org/resource/>\r\n"
			+ "PREFIX  co:   <http://purl.org/ontology/co/core#>\r\n"
			+ "PREFIX  dbpr: <http://dbpedia.org/resource/>\r\n"
			+ "PREFIX  dbo:  <http://dbpedia.org/ontology/>\r\n"
			+ "PREFIX  arco: <http://www.gate.ac.uk/ns/ontologies/arcomem-data-model.owl#>\r\n"
			+ "PREFIX  dbp:  <http://dbpedia.org/property/>\r\n"
			+ "PREFIX  rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\r\n"
			+ "PREFIX  sesame: <http://www.openrdf.org/schema/sesame#>\r\n"
			+ "PREFIX  category: <http://dbpedia.org/resource/Category:>\r\n"
			+ "PREFIX  bif:  <http://www.openlinksw.com/schemas/bif#>\r\n"
			+ "PREFIX  db:   <http://dbpedia.org/>\r\n"
			+ "PREFIX  dc:   <http://purl.org/dc/elements/1.1/>\r\n"
			+ "\r\n"
			+ "SELECT DISTINCT  ?var1\n"
			+ "WHERE\n"
			+ "{ ?var2  <http://www.wikidata.org/prop/direct/P40>  <http://www.wikidata.org/entity/Q76> ;\n"
			+ "?var1                 <http://www.wikidata.org/entity/Q18643532>\n"
			+ "}";
	
	public static void main(String[] args) throws Exception {
		// Read my TDB dataset
		String dbDir = "D:\\tmp\\WikiDB";
		Dataset ds = TDBFactory.createDataset(dbDir);
		// Write if I wanna write, but I'll be using read to query over it mostly
		ds.begin(ReadWrite.READ);
		// Define model and Query
		Model model = ds.getDefaultModel();
		
		Query q1 = QueryFactory.create(s1);
		Op alg = Algebra.compile(q1);
		alg = Algebra.optimize(alg);
		QueryIterator qit = Algebra.exec(alg, model);
		int resultAmount = 0;
		
		while (qit.hasNext()) {
			qit.next();
			resultAmount++;
		}
		
		System.out.println(resultAmount);
	}
}
