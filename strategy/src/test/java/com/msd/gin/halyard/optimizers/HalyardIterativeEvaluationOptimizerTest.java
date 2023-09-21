package com.msd.gin.halyard.optimizers;

import com.msd.gin.halyard.algebra.NAryUnion;

import java.util.Arrays;

import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.QueryRoot;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Union;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.helpers.TupleExprs;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class HalyardIterativeEvaluationOptimizerTest {
	private StatementPattern createStatementPattern(String s, String o) {
		return new StatementPattern(new Var("s"), TupleExprs.createConstVar(RDF.VALUE), new Var("o"));
	}

	@Test
	public void testUnionEstimates() {
		StatementPattern sp1 = createStatementPattern("s", "p");
		sp1.setResultSizeEstimate(25);
		StatementPattern sp2 = createStatementPattern("s", "x");
		sp2.setResultSizeEstimate(2);
		StatementPattern sp3 = createStatementPattern("s", "y");
		sp3.setResultSizeEstimate(3);
		Join j1 = new Join(sp1, sp2);
		j1.setResultSizeEstimate(j1.getLeftArg().getResultSizeEstimate() * j1.getRightArg().getResultSizeEstimate());
		Join j2 = new Join(sp1, sp3);
		j2.setResultSizeEstimate(j2.getLeftArg().getResultSizeEstimate() * j2.getRightArg().getResultSizeEstimate());
		Union union = new Union(j1, j2);
		union.setResultSizeEstimate(union.getLeftArg().getResultSizeEstimate() + union.getRightArg().getResultSizeEstimate());
		QueryRoot root = new QueryRoot(union);
		double total = union.getResultSizeEstimate();
		new HalyardIterativeEvaluationOptimizer().optimize(root, null, null);
		assertEquals(total, root.getArg().getResultSizeEstimate(), root.toString());
		assertEquals(sp2.getResultSizeEstimate() + sp3.getResultSizeEstimate(), union.getResultSizeEstimate(), root.toString());
	}

	@Test
	public void testNAryUnionEstimates() {
		StatementPattern sp1 = createStatementPattern("s", "p");
		sp1.setResultSizeEstimate(25);
		StatementPattern sp2 = createStatementPattern("s", "x");
		sp2.setResultSizeEstimate(2);
		StatementPattern sp3 = createStatementPattern("s", "y");
		sp3.setResultSizeEstimate(3);
		StatementPattern sp4 = createStatementPattern("s", "z");
		sp3.setResultSizeEstimate(4);
		Join j1 = new Join(sp1, sp2);
		j1.setResultSizeEstimate(j1.getLeftArg().getResultSizeEstimate() * j1.getRightArg().getResultSizeEstimate());
		Join j2 = new Join(sp1, sp3);
		j2.setResultSizeEstimate(j2.getLeftArg().getResultSizeEstimate() * j2.getRightArg().getResultSizeEstimate());
		Join j3 = new Join(sp1, sp4);
		j3.setResultSizeEstimate(j3.getLeftArg().getResultSizeEstimate() * j3.getRightArg().getResultSizeEstimate());
		NAryUnion union = new NAryUnion(Arrays.asList(j1, j2, j3));
		union.setResultSizeEstimate(union.getArgs().stream().mapToDouble(QueryModelNode::getResultSizeEstimate).sum());
		QueryRoot root = new QueryRoot(union);
		double total = union.getResultSizeEstimate();
		new HalyardIterativeEvaluationOptimizer().optimize(root, null, null);
		assertEquals(total, root.getArg().getResultSizeEstimate(), root.toString());
		assertEquals(sp2.getResultSizeEstimate() + sp3.getResultSizeEstimate() + sp4.getResultSizeEstimate(), union.getResultSizeEstimate(), root.toString());
	}
}
