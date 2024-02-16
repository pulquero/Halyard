package com.msd.gin.halyard.query.algebra;

import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class NAryUnionTest {
	@Test
	public void testBindingNames() {
		int N = 3;
		List<TupleExpr> children = new ArrayList<>();
		for (int j=0; j<N; j++) {
			children.add(new StatementPattern(new Var("s"), Algebra.createAnonVar("p"+j), Algebra.createAnonVar("o"+j)));
		}
		NAryUnion nu = new NAryUnion(Arrays.asList(
			new StatementPattern(new Var("s"), Algebra.createAnonVar("p1"), Algebra.createAnonVar("o1")),
			new StatementPattern(new Var("s"), Algebra.createAnonVar("p2"), Algebra.createAnonVar("o2"))
		));
		assertEquals(Sets.newHashSet("s", "p1", "o1", "p2", "o2"), nu.getBindingNames());
	}

	@Test
	public void testAssuredBindingNames() {
		int N = 3;
		List<TupleExpr> children = new ArrayList<>();
		for (int j=0; j<N; j++) {
			children.add(new StatementPattern(new Var("s"), Algebra.createAnonVar("p"+j), Algebra.createAnonVar("o"+j)));
		}
		NAryUnion nu = new NAryUnion(children);
		assertEquals(Sets.newHashSet("s"), nu.getAssuredBindingNames());
	}

	@Test
	public void testRemove() {
		int N = 3;
		for (int i=0; i<N; i++) {
			List<TupleExpr> children = new ArrayList<>();
			for (int j=0; j<N; j++) {
				children.add(new StatementPattern(new Var("s"), Algebra.createAnonVar("p"+j), Algebra.createAnonVar("o"+j)));
			}
			NAryUnion nu = new NAryUnion(children);
			nu.removeChildNode(children.remove(i));
			assertEquals(children, nu.getArgs());
		}
	}

	@Test
	public void testRemoveNonExistent() {
		int N = 3;
		List<TupleExpr> children = new ArrayList<>();
		for (int j=0; j<N; j++) {
			children.add(new StatementPattern(new Var("s"), Algebra.createAnonVar("p"+j), Algebra.createAnonVar("o"+j)));
		}
		NAryUnion nu = new NAryUnion(children);
		nu.removeChildNode(new StatementPattern(new Var("s"), Algebra.createAnonVar("p"), Algebra.createAnonVar("o")));
		assertEquals(children, nu.getArgs());
	}

	@Test
	public void testHashCode() {
		StatementPattern sp1 = new StatementPattern(new Var("s"), Algebra.createAnonVar("p1"), Algebra.createAnonVar("o1"));
		StatementPattern sp2 = new StatementPattern(new Var("s"), Algebra.createAnonVar("p2"), Algebra.createAnonVar("o2"));
		NAryUnion nu12 = new NAryUnion(Arrays.asList(sp1, sp2));
		NAryUnion nu21 = new NAryUnion(Arrays.asList(sp2, sp1));
		assertEquals(nu12.hashCode(), nu21.hashCode());
	}
}
