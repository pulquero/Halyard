package com.msd.gin.halyard.queryparser;

import com.msd.gin.halyard.common.IdValueFactory;
import com.msd.gin.halyard.common.RDFFactory;

import org.apache.hadoop.conf.Configuration;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.ParentReferenceChecker;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.junit.jupiter.api.Test;

public class SPARQLParserTest {
	@Test
	public void testSerializationDateTime() {
		ParsedQuery q = SPARQLParser.parseQuery("select * where {bind(\"2002-05-30T09:30:10.2\"^^xsd:dateTime as ?t)}", null, new IdValueFactory(RDFFactory.create(new Configuration())));
		new ParentReferenceChecker(null).optimize(q.getTupleExpr(), q.getDataset(), null);
	}

	@Test
	public void testSerializationInteger() {
		ParsedQuery q = SPARQLParser.parseQuery("select * where {bind(7 as ?t)}", null, new IdValueFactory(RDFFactory.create(new Configuration())));
		new ParentReferenceChecker(null).optimize(q.getTupleExpr(), q.getDataset(), null);
	}

	@Test
	public void testSerializationBoolean() {
		ParsedQuery q = SPARQLParser.parseQuery("select * where {bind(true as ?t)}", null, new IdValueFactory(RDFFactory.create(new Configuration())));
		new ParentReferenceChecker(null).optimize(q.getTupleExpr(), q.getDataset(), null);
	}
}
