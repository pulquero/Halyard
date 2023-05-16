package com.msd.gin.halyard.algebra;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class StarJoinTest {
	@Test
	public void testRemove() {
		int N = 3;
		for (int i=0; i<N; i++) {
			List<StatementPattern> children = new ArrayList<>();
			for (int j=0; j<N; j++) {
				children.add(new StatementPattern(new Var("s"), Algebra.createAnonVar("p"+j), Algebra.createAnonVar("o"+j)));
			}
			StarJoin sj = new StarJoin(new Var("s"), null, children);
			sj.removeChildNode(children.remove(i));
			assertEquals(children, sj.getArgs());
		}
	}

	@Test
	public void testRemoveNonExistent() {
		int N = 3;
		List<StatementPattern> children = new ArrayList<>();
		for (int j=0; j<N; j++) {
			children.add(new StatementPattern(new Var("s"), Algebra.createAnonVar("p"+j), Algebra.createAnonVar("o"+j)));
		}
		StarJoin sj = new StarJoin(new Var("s"), null, children);
		sj.removeChildNode(new StatementPattern(new Var("s"), Algebra.createAnonVar("p"), Algebra.createAnonVar("o")));
		assertEquals(children, sj.getArgs());
	}

	@Test
	public void testHashCode() {
		StatementPattern sp1 = new StatementPattern(new Var("s"), Algebra.createAnonVar("p1"), Algebra.createAnonVar("o1"));
		StatementPattern sp2 = new StatementPattern(new Var("s"), Algebra.createAnonVar("p2"), Algebra.createAnonVar("o2"));
		StarJoin sj12 = new StarJoin(new Var("s"), null, Arrays.asList(sp1, sp2));
		StarJoin sj21 = new StarJoin(new Var("s"), null, Arrays.asList(sp2, sp1));
		assertEquals(sj12.hashCode(), sj21.hashCode());
	}
}
