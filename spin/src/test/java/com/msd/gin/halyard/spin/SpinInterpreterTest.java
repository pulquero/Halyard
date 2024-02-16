package com.msd.gin.halyard.spin;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.function.Supplier;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.SPIF;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunctionRegistry;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.ExtendedEvaluationStrategyFactory;
import org.eclipse.rdf4j.query.parser.ParsedTupleQuery;
import org.eclipse.rdf4j.query.parser.QueryParserUtil;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.junit.Test;

import com.msd.gin.halyard.query.algebra.Algebra;
import com.msd.gin.halyard.query.algebra.evaluation.CloseableTripleSource;
import com.msd.gin.halyard.sail.connection.SailConnectionTripleSource;

public class SpinInterpreterTest {

	@Test
	public void testNativeInterpreter_empty() throws RDFParseException, RepositoryException, IOException {
		MemoryStore sail = new MemoryStore();
		sail.setEvaluationStrategyFactory(new ExtendedEvaluationStrategyFactory());
		SailRepository repo = new SailRepository(sail);
		repo.init();
		SailRepositoryConnection repoConn = repo.getConnection();
		SpinInferencing.insertSchema(repoConn);

		SpinMagicPropertyInterpreter interpreter = new SpinMagicPropertyInterpreter(new SpinParser(), new SailConnectionTripleSource(sail.getConnection(), true, sail.getValueFactory()), new TupleFunctionRegistry(), sail.getFederatedServiceResolver());
		ParsedTupleQuery q = QueryParserUtil.parseTupleQuery(QueryLanguage.SPARQL, "prefix spif: <http://spinrdf.org/spif#> " + "select ?str {?str spif:split (\"Hello World\" \" \")}", null);
		TupleExpr expr = Algebra.ensureRooted(q.getTupleExpr());
		interpreter.optimize(expr, null, null);
		try (CloseableIteration<? extends BindingSet, QueryEvaluationException> iter = repoConn.getSailConnection().evaluate(expr, null, new QueryBindingSet(), true)) {
			assertEquals("Hello", iter.next().getValue("str").stringValue());
			assertEquals("World", iter.next().getValue("str").stringValue());
		}
	}

	@Test
	public void testSpinServiceInterpreter_empty() throws RDFParseException, RepositoryException, IOException {
		MemoryStore sail = new MemoryStore();
		SailRepository repo = new SailRepository(sail);
		repo.init();
		SailRepositoryConnection repoConn = repo.getConnection();
		SpinInferencing.insertSchema(repoConn);

		Supplier<CloseableTripleSource> tsFactory = () -> new SailConnectionTripleSource(sail.getConnection(), true, sail.getValueFactory());
		SpinMagicPropertyInterpreter interpreter = new SpinMagicPropertyInterpreter(new SpinParser(), tsFactory.get(), new TupleFunctionRegistry(), sail.getFederatedServiceResolver(), tsFactory, true);
		ParsedTupleQuery q = QueryParserUtil.parseTupleQuery(QueryLanguage.SPARQL, "prefix spif: <http://spinrdf.org/spif#> " + "select ?str {?str spif:split (\"Hello World\" \" \")}", null);
		TupleExpr expr = Algebra.ensureRooted(q.getTupleExpr());
		interpreter.optimize(expr, null, null);
		try (CloseableIteration<? extends BindingSet, QueryEvaluationException> iter = repoConn.getSailConnection().evaluate(expr, null, new QueryBindingSet(), true)) {
			assertEquals("Hello", iter.next().getValue("str").stringValue());
			assertEquals("World", iter.next().getValue("str").stringValue());
		}
	}

	@Test
	public void testNativeInterpreter_nonempty() throws RDFParseException, RepositoryException, IOException {
		MemoryStore sail = new MemoryStore();
		sail.setEvaluationStrategyFactory(new ExtendedEvaluationStrategyFactory());
		SailRepository repo = new SailRepository(sail);
		repo.init();
		SailRepositoryConnection repoConn = repo.getConnection();
		ValueFactory vf = repoConn.getValueFactory();
		SpinInferencing.insertSchema(repoConn);
		repoConn.add(vf.createBNode(), vf.createIRI(SPIF.NAMESPACE, "split"), vf.createLiteral("decoy"));

		SpinMagicPropertyInterpreter interpreter = new SpinMagicPropertyInterpreter(new SpinParser(), new SailConnectionTripleSource(sail.getConnection(), true, sail.getValueFactory()), new TupleFunctionRegistry(), sail.getFederatedServiceResolver());
		ParsedTupleQuery q = QueryParserUtil.parseTupleQuery(QueryLanguage.SPARQL, "prefix spif: <http://spinrdf.org/spif#> " + "select ?str {?str spif:split (\"Hello World\" \" \")}", null);
		TupleExpr expr = Algebra.ensureRooted(q.getTupleExpr());
		interpreter.optimize(expr, null, null);
		try (CloseableIteration<? extends BindingSet, QueryEvaluationException> iter = repoConn.getSailConnection().evaluate(expr, null, new QueryBindingSet(), true)) {
			assertEquals("Hello", iter.next().getValue("str").stringValue());
			assertEquals("World", iter.next().getValue("str").stringValue());
		}
	}

	@Test
	public void testSpinServiceInterpreter_nonempty() throws RDFParseException, RepositoryException, IOException {
		MemoryStore sail = new MemoryStore();
		SailRepository repo = new SailRepository(sail);
		repo.init();
		SailRepositoryConnection repoConn = repo.getConnection();
		ValueFactory vf = repoConn.getValueFactory();
		SpinInferencing.insertSchema(repoConn);
		repoConn.add(vf.createBNode(), vf.createIRI(SPIF.NAMESPACE, "split"), vf.createLiteral("decoy"));

		Supplier<CloseableTripleSource> tsFactory = () -> new SailConnectionTripleSource(sail.getConnection(), true, sail.getValueFactory());
		SpinMagicPropertyInterpreter interpreter = new SpinMagicPropertyInterpreter(new SpinParser(), tsFactory.get(), new TupleFunctionRegistry(), sail.getFederatedServiceResolver(), tsFactory, true);
		ParsedTupleQuery q = QueryParserUtil.parseTupleQuery(QueryLanguage.SPARQL, "prefix spif: <http://spinrdf.org/spif#> " + "select ?str {?str spif:split (\"Hello World\" \" \")}", null);
		TupleExpr expr = Algebra.ensureRooted(q.getTupleExpr());
		interpreter.optimize(expr, null, null);
		try (CloseableIteration<? extends BindingSet, QueryEvaluationException> iter = repoConn.getSailConnection().evaluate(expr, null, new QueryBindingSet(), true)) {
			assertEquals("Hello", iter.next().getValue("str").stringValue());
			assertEquals("World", iter.next().getValue("str").stringValue());
		}
	}
}
