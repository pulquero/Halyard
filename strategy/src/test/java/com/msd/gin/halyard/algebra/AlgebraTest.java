package com.msd.gin.halyard.algebra;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.UnaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.Var;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AlgebraTest {
	@Test
	public void testRemove() {
		int N = 3;
		for (int i=0; i<N; i++) {
			List<StatementPattern> children = new ArrayList<>();
			for (int j=0; j<N; j++) {
				children.add(new StatementPattern(new Var("s"), Algebra.createAnonVar("p"+j), Algebra.createAnonVar("o"+j)));
			}
			StarJoin sj = new StarJoin(new Var("s"), null, children);
			UnaryTupleOperator root = Algebra.ensureRooted(sj);
			Algebra.remove(children.remove(i));
			assertEquals(children, sj.getArgs());
		}
	}

	@Test
	public void testRemoveLeavesSingleArgStarJoin() {
		List<StatementPattern> children = new ArrayList<>();
		children.add(new StatementPattern(new Var("s"), Algebra.createAnonVar("p"), Algebra.createAnonVar("o")));
		children.add(new StatementPattern(new Var("s"), Algebra.createAnonVar("u"), Algebra.createAnonVar("v")));
		StarJoin sj = new StarJoin(new Var("s"), null, children);
		UnaryTupleOperator root = Algebra.ensureRooted(sj);
		Algebra.remove(children.get(0));
		assertEquals(children.get(1), root.getArg());
	}
}
