package com.msd.gin.halyard.strategy;

import com.msd.gin.halyard.algebra.ServiceRoot;

import java.util.Collections;

import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.QueryRoot;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TupleExprPriorityAssignerTest {
	private TupleExprPriorityAssigner priorityAssigner;

	private StatementPattern createStatementPattern(String s, String p, String o) {
		return new StatementPattern(new Var(s), new Var(p), new Var(o));
	}

	private StatementPattern createStatementPattern(int i) {
		return createStatementPattern("s"+i, "p"+i, "o"+i);
	}

	@BeforeEach
	public void setUp() {
		priorityAssigner = new TupleExprPriorityAssigner();
	}

	@Test
	public void testJoinPriority() {
		StatementPattern sp1 = createStatementPattern(1);
		StatementPattern sp2 = createStatementPattern(2);
		StatementPattern sp3 = createStatementPattern(3);
		Join j2 = new Join(sp2, sp3);
		Join j1 = new Join(sp1, j2);
		QueryRoot root = new QueryRoot(j1);
		assertEquals(1, priorityAssigner.getPriority(j1));
		assertEquals(2, priorityAssigner.getPriority(sp1));
		assertEquals(3, priorityAssigner.getPriority(j2));
		assertEquals(4, priorityAssigner.getPriority(sp2));
		assertEquals(5, priorityAssigner.getPriority(sp3));
	}

	@Test
	public void testServicePriority() {
		StatementPattern sp1 = createStatementPattern(1);
		StatementPattern sp2 = createStatementPattern(2);
		StatementPattern sp3 = createStatementPattern(3);
		StatementPattern sp4 = createStatementPattern(3);
		Join j3 = new Join(sp3, sp4);
		Service service = new Service(new Var("url"), j3, "# query string not used by test", Collections.emptyMap(), null, false);
		Join j2 = new Join(sp2, service);
		Join j1 = new Join(sp1, j2);
		QueryRoot root = new QueryRoot(j1);
		assertEquals(1, priorityAssigner.getPriority(j1));
		assertEquals(2, priorityAssigner.getPriority(sp1));
		assertEquals(3, priorityAssigner.getPriority(j2));
		assertEquals(4, priorityAssigner.getPriority(sp2));
		assertEquals(5, priorityAssigner.getPriority(service));
		assertEquals(6, priorityAssigner.getPriority(j3));
		assertEquals(7, priorityAssigner.getPriority(sp3));
		assertEquals(8, priorityAssigner.getPriority(sp4));

		ServiceRoot serviceRoot = ServiceRoot.create(service);
		Join serviceJoin = (Join) serviceRoot.getArg();
		assertEquals(6, priorityAssigner.getPriority(serviceJoin));
		assertEquals(7, priorityAssigner.getPriority(serviceJoin.getLeftArg()));
		assertEquals(8, priorityAssigner.getPriority(serviceJoin.getRightArg()));
	}

	@Test
	public void testPrintQueryNode() {
		HalyardEvaluationExecutor.printQueryNode(createStatementPattern(1), EmptyBindingSet.getInstance());
	}
}
