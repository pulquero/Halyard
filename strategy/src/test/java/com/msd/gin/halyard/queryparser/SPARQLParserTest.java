package com.msd.gin.halyard.queryparser;

import com.msd.gin.halyard.common.IdValueFactory;
import com.msd.gin.halyard.common.RDFFactory;
import com.msd.gin.halyard.query.algebra.AbstractExtendedQueryModelVisitor;
import com.msd.gin.halyard.query.algebra.BGPCollector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.Modify;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TripleRef;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.ParentReferenceChecker;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.ParsedUpdate;
import org.junit.jupiter.api.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SPARQLParserTest {
	private ValueFactory vf = new IdValueFactory(RDFFactory.create(new Configuration()));

	@Test
	public void testSerializationDateTime() {
		ParsedQuery q = SPARQLParser.parseQuery("select * where {bind(\"2002-05-30T09:30:10.2\"^^xsd:dateTime as ?t)}", null, vf);
		new ParentReferenceChecker(null).optimize(q.getTupleExpr(), q.getDataset(), null);
	}

	@Test
	public void testSerializationInteger() {
		ParsedQuery q = SPARQLParser.parseQuery("select * where {bind(7 as ?t)}", null, vf);
		new ParentReferenceChecker(null).optimize(q.getTupleExpr(), q.getDataset(), null);
	}

	@Test
	public void testSerializationBoolean() {
		ParsedQuery q = SPARQLParser.parseQuery("select * where {bind(true as ?t)}", null, vf);
		new ParentReferenceChecker(null).optimize(q.getTupleExpr(), q.getDataset(), null);
	}

	@Test
	public void testInsertWithTripleUsesConstantVarName() {
		ParsedUpdate u = SPARQLParser.parseUpdate("insert {<<?s rdf:value ?v>> a ?t} where { values (?s ?v ?t) { (<:subj> 6 <:Data>) }}", null, vf);
		Modify modify = (Modify) u.getUpdateExprs().get(0);
		String varName = ((StatementPattern) modify.getInsertExpr()).getSubjectVar().getName();
		// verify TripleRef var name doesn't change with parse invocations
		// so that if the WHERE clause is cached it still matches up with that used in the INSERT
		for (int i=0; i<3; i++) {
			ParsedUpdate u_i = SPARQLParser.parseUpdate("insert {<<?s rdf:value ?v>> a ?t} where { values (?s ?v ?t) { (<:subj> 6 <:Data>) }}", null, vf);
			Modify modify_i = (Modify) u_i.getUpdateExprs().get(0);
			String varName_i = ((StatementPattern) modify_i.getInsertExpr()).getSubjectVar().getName();
			assertEquals(varName, varName_i);
		}
	}

	@Test
	public void testListWithNestedTriple() {
		ParsedQuery q = SPARQLParser.parseQuery("select * where {(?a << ?s <:pred> <:obj> >> <:z>) a rdf:List}", null, vf);
		q.getTupleExpr().visit(new AbstractExtendedQueryModelVisitor<>() {
			@Override
			public void meet(Join node) throws RuntimeException {
				final List<TripleRef> tripleRefs = new ArrayList<>();
				BGPCollector<RuntimeException> bgp = new BGPCollector<>(this) {
					@Override
					public void meet(TripleRef tr) {
						tripleRefs.add(tr);
					}
				};
				node.visit(bgp);
				Map<String,StatementPattern> firstsBySubj = new HashMap<>();
				Map<String,StatementPattern> restsBySubj = new HashMap<>();
				String startVar = null;
				for (StatementPattern sp : bgp.getStatementPatterns()) {
					IRI pred = (IRI) sp.getPredicateVar().getValue();
					if (RDF.FIRST.equals(pred)) {
						firstsBySubj.put(sp.getSubjectVar().getName(), sp);
					} else if (RDF.REST.equals(pred)) {
						restsBySubj.put(sp.getSubjectVar().getName(), sp);
					} else if (RDF.LIST.equals(sp.getObjectVar().getValue())) {
						startVar = sp.getSubjectVar().getName();
					}
				}
				Map<String,TripleRef> trefsByRef = new HashMap<>();
				for (TripleRef tref : tripleRefs) {
					trefsByRef.put(tref.getExprVar().getName(), tref);
				}
				StatementPattern sp1 = firstsBySubj.get(startVar);
				assertEquals("a", sp1.getObjectVar().getName());
				StatementPattern sp2 = restsBySubj.get(sp1.getSubjectVar().getName());
				StatementPattern sp3 = firstsBySubj.get(sp2.getObjectVar().getName());
				TripleRef tref = trefsByRef.get(sp3.getObjectVar().getName());
				assertNotNull(tref);
			}
		});
	}
}
