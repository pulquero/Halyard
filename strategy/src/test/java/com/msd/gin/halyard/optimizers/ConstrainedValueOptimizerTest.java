package com.msd.gin.halyard.optimizers;

import com.msd.gin.halyard.common.StatementIndex;
import com.msd.gin.halyard.model.TermRole;
import com.msd.gin.halyard.model.vocabulary.HALYARD;
import com.msd.gin.halyard.query.algebra.ConstrainedStatementPattern;
import com.msd.gin.halyard.query.algebra.VarConstraint;

import java.math.BigInteger;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.algebra.Datatype;
import org.eclipse.rdf4j.query.algebra.Extension;
import org.eclipse.rdf4j.query.algebra.ExtensionElem;
import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.FunctionCall;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.LeftJoin;
import org.eclipse.rdf4j.query.algebra.Projection;
import org.eclipse.rdf4j.query.algebra.ProjectionElem;
import org.eclipse.rdf4j.query.algebra.ProjectionElemList;
import org.eclipse.rdf4j.query.algebra.QueryRoot;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.ValueConstant;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.junit.jupiter.api.Test;

public class ConstrainedValueOptimizerTest extends AbstractOptimizerTest {
	private static final ValueFactory VF = SimpleValueFactory.getInstance();

	@Override
	protected QueryOptimizer getOptimizer() {
		return new ConstrainedValueOptimizer();
	}

	@Test
	public void testMultipleStatements() {
		String q ="PREFIX halyard: <"+HALYARD.NAMESPACE+">\n"
				+"SELECT ?s { ?s ?p ?x. ?s ?q ?y. filter(halyard:forkAndFilterBy(2,?s)) }";
		ConstrainedStatementPattern csp = new ConstrainedStatementPattern(StatementPattern.Scope.DEFAULT_CONTEXTS, new Var("s"), new Var("p"), new Var("x"), null, StatementIndex.Name.SPO, TermRole.SUBJECT, VarConstraint.partitionConstraint(2));
		StatementPattern sp = new StatementPattern(StatementPattern.Scope.DEFAULT_CONTEXTS, new Var("s"), new Var("q"), new Var("y"), null);
		TupleExpr where = new Join(csp, sp);
		TupleExpr expected = new QueryRoot(
				new Projection(where, new ProjectionElemList(new ProjectionElem("s"))));
		testOptimizer(expected, q);
	}

	@Test
	public void testMultiplePartitioning() {
		String q ="PREFIX halyard: <"+HALYARD.NAMESPACE+">\n"
				+"SELECT ?s { ?s ?p ?o filter(halyard:forkAndFilterBy(2,?s)) filter(halyard:forkAndFilterBy(2,?o)) }";
		ConstrainedStatementPattern csp = new ConstrainedStatementPattern(StatementPattern.Scope.DEFAULT_CONTEXTS, new Var("s"), new Var("p"), new Var("o"), null, StatementIndex.Name.OSP, TermRole.OBJECT, VarConstraint.partitionConstraint(2));
		TupleExpr where = new Filter(csp, new FunctionCall(HALYARD.PARALLEL_SPLIT_FUNCTION.stringValue(), new ValueConstant(VF.createLiteral(BigInteger.valueOf(2))), new Var("s")));
		TupleExpr expected = new QueryRoot(
				new Projection(where, new ProjectionElemList(new ProjectionElem("s"))));
		testOptimizer(expected, q);
	}

	@Test
	public void testFromDataset() {
		String q ="PREFIX halyard: <"+HALYARD.NAMESPACE+">\n"
				+"SELECT ?s FROM <http://whatever/graph> { ?s ?p ?o filter(halyard:forkAndFilterBy(2,?p)) }";
		ConstrainedStatementPattern csp = new ConstrainedStatementPattern(StatementPattern.Scope.DEFAULT_CONTEXTS, new Var("s"), new Var("p"), new Var("o"), null, StatementIndex.Name.CPOS, TermRole.PREDICATE, VarConstraint.partitionConstraint(2));
		TupleExpr where = csp;
		TupleExpr expected = new QueryRoot(
				new Projection(where, new ProjectionElemList(new ProjectionElem("s"))));
		testOptimizer(expected, q);
	}

	@Test
	public void testOptional() {
		String q ="PREFIX halyard: <"+HALYARD.NAMESPACE+">\n"
				+"SELECT ?s { ?s ?p ?x filter(halyard:forkAndFilterBy(2,?s)) OPTIONAL { ?s ?q ?y } }";
		ConstrainedStatementPattern csp = new ConstrainedStatementPattern(StatementPattern.Scope.DEFAULT_CONTEXTS, new Var("s"), new Var("p"), new Var("x"), null, StatementIndex.Name.SPO, TermRole.SUBJECT, VarConstraint.partitionConstraint(2));
		StatementPattern sp = new StatementPattern(StatementPattern.Scope.DEFAULT_CONTEXTS, new Var("s"), new Var("q"), new Var("y"));
		TupleExpr where = new LeftJoin(csp, sp);
		TupleExpr expected = new QueryRoot(
				new Projection(where, new ProjectionElemList(new ProjectionElem("s"))));
		testOptimizer(expected, q);
	}

	@Test
	public void testBind() {
		String q ="PREFIX halyard: <"+HALYARD.NAMESPACE+">\n"
				+"SELECT ?s { ?s ?p ?x filter(halyard:forkAndFilterBy(2,?s)) BIND(datatype(?x) as ?y) }";
		ConstrainedStatementPattern csp = new ConstrainedStatementPattern(StatementPattern.Scope.DEFAULT_CONTEXTS, new Var("s"), new Var("p"), new Var("x"), null, StatementIndex.Name.SPO, TermRole.SUBJECT, VarConstraint.partitionConstraint(2));
		Extension ext = new Extension(csp, new ExtensionElem(new Datatype(new Var("x")), "y"));
		TupleExpr where = ext;
		TupleExpr expected = new QueryRoot(
				new Projection(where, new ProjectionElemList(new ProjectionElem("s"))));
		testOptimizer(expected, q);
	}
}
