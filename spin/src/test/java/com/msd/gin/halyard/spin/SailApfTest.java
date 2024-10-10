package com.msd.gin.halyard.spin;

import static org.junit.Assert.assertTrue;

import java.util.List;

import org.eclipse.rdf4j.common.iteration.Iterations;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.BooleanQuery;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.Sail;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class SailApfTest {

	private Repository repo;

	private RepositoryConnection conn;

	@Before
	public final void setup() throws Exception {
		repo = new SailRepository(createSail());
		repo.init();
		conn = repo.getConnection();
	}

	protected Sail createSail() throws Exception {
		return new SpinMemoryStore();
	}

	@After
	public final void tearDown() throws Exception {
		if (conn != null) {
			conn.close();
		}
		if (repo != null) {
			repo.shutDown();
		}
		postCleanup();
	}

	protected void postCleanup() throws Exception {
	}

	@Test
	public void testConcat() throws Exception {
		TupleQuery tq = conn.prepareTupleQuery(QueryLanguage.SPARQL, "prefix apf: <http://jena.apache.org/ARQ/property#>\n" + "\n" + "select ?text where {\n" + "   ?text apf:concat (\"very\" \"sour\" \"berry\") . }");
		try (TupleQueryResult tqresult = tq.evaluate()) {
			Assert.assertEquals("verysourberry", tqresult.next().getValue("text").stringValue());
		}
	}

	@Test
	public void testStrSplit() throws Exception {
		TupleQuery tq = conn.prepareTupleQuery(QueryLanguage.SPARQL, "prefix apf: <http://jena.apache.org/ARQ/property#>\n" + "\n" + "select ?text where {\n" + "   ?text apf:strSplit (\"very:sour:berry\" \":\") . }");
		try (TupleQueryResult tqr = tq.evaluate()) {
			List<BindingSet> resultList = Iterations.asList(tqr);
			List<String> resultStringList = Lists.transform(resultList, (BindingSet input) -> input.getValue("text").stringValue());
	
			Assert.assertArrayEquals(new String[] { "very", "sour", "berry" }, resultStringList.toArray(new String[resultStringList.size()]));
		}
	}

	@Test
	public void testSplitIRI() throws Exception {
		BooleanQuery bq = conn.prepareBooleanQuery(QueryLanguage.SPARQL, "prefix apf: <http://jena.apache.org/ARQ/property#> " + "ask where { apf:foobar apf:splitIRI (?ns ?ln) filter(?ns = str(apf:) && ?ln = 'foobar')}");
		assertTrue(bq.evaluate());
	}
}
