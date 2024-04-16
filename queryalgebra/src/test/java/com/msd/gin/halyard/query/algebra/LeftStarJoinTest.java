package com.msd.gin.halyard.query.algebra;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.eclipse.rdf4j.query.algebra.SingletonSet;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.junit.jupiter.api.Test;

public class LeftStarJoinTest {
	@Test
	public void testRemove() {
		int N = 3;
		SingletonSet base = new SingletonSet();
		for (int i=0; i<N; i++) {
			List<StatementPattern> sps = new ArrayList<>();
			for (int j=0; j<N; j++) {
				sps.add(new StatementPattern(new Var("s"), Algebra.createAnonVar("p"+j), Algebra.createAnonVar("o"+j)));
			}
			LeftStarJoin sj = new LeftStarJoin(new Var("s"), null, base, sps);

			List<TupleExpr> children = new ArrayList<>();
			children.add(base);
			children.addAll(sps);
			sj.removeChildNode(children.remove(i));
			assertEquals(children, sj.getArgs());
		}
	}

	@Test
	public void testRemoveNonExistent() {
		int N = 3;
		SingletonSet base = new SingletonSet();
		List<StatementPattern> sps = new ArrayList<>();
		for (int j=0; j<N; j++) {
			sps.add(new StatementPattern(new Var("s"), Algebra.createAnonVar("p"+j), Algebra.createAnonVar("o"+j)));
		}
		LeftStarJoin sj = new LeftStarJoin(new Var("s"), null, base, sps);
		sj.removeChildNode(new StatementPattern(new Var("s"), Algebra.createAnonVar("p"), Algebra.createAnonVar("o")));

		List<TupleExpr> children = new ArrayList<>();
		children.add(base);
		children.addAll(sps);
		assertEquals(children, sj.getArgs());
	}

	@Test
	public void testHashCode() {
		StatementPattern sp1 = new StatementPattern(new Var("s"), Algebra.createAnonVar("p1"), Algebra.createAnonVar("o1"));
		StatementPattern sp2 = new StatementPattern(new Var("s"), Algebra.createAnonVar("p2"), Algebra.createAnonVar("o2"));
		LeftStarJoin sj12 = new LeftStarJoin(new Var("s"), null, sp1, Arrays.asList(sp2));
		LeftStarJoin sj21 = new LeftStarJoin(new Var("s"), null, sp2, Arrays.asList(sp1));
		assertEquals(sj12.hashCode(), sj21.hashCode());
	}
}
