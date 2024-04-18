package com.msd.gin.halyard.optimizers;

import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.algebra.EmptySet;
import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.Projection;
import org.eclipse.rdf4j.query.algebra.SingletonSet;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Union;
import org.eclipse.rdf4j.query.algebra.ValueConstant;
import org.eclipse.rdf4j.query.algebra.Var;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ExtendedQueryModelNormalizerTest extends AbstractOptimizerTest {

	private ExtendedQueryModelNormalizer subject;

	@BeforeEach
	public void setup() throws Exception {
		subject = getOptimizer();
	}

	@Test
	public void testNormalizeUnionWithEmptyLeft() {
		Projection p = new Projection();
		Union union = new Union();
		SingletonSet s = new SingletonSet();
		union.setLeftArg(new EmptySet());
		union.setRightArg(s);
		p.setArg(union);

		subject.meet(union);

		assertThat(p.getArg()).isEqualTo(s);
	}

	@Test
	public void testNormalizeUnionWithEmptyRight() {
		Projection p = new Projection();
		Union union = new Union();
		SingletonSet s = new SingletonSet();
		union.setRightArg(new EmptySet());
		union.setLeftArg(s);
		p.setArg(union);

		subject.meet(union);

		assertThat(p.getArg()).isEqualTo(s);
	}

	/**
	 * @see <a href="https://github.com/eclipse/rdf4j/issues/1404">GH-1404</a>
	 */
	@Test
	public void testNormalizeUnionWithTwoSingletons() {
		Projection p = new Projection();
		Union union = new Union();
		union.setRightArg(new SingletonSet());
		union.setLeftArg(new SingletonSet());
		p.setArg(union);

		subject.meet(union);

		assertThat(p.getArg()).isEqualTo(union);
	}

	@Test
	public void testNormalizeFilterWithUnion() {
		Union union = new Union(new StatementPattern(new Var("s"), new Var("p"), new Var("o")), new StatementPattern(new Var("x"), new Var("q"), new Var("o")));
		Filter filter = new Filter(union, new Var("o"));
		Projection p = new Projection(filter);

		subject.meet(filter);

		assertThat(p.getArg()).isEqualTo(union);
	}

	@Test
	public void testNormalizeFilterWithMultipleUnions() {
		Union union1 = new Union(new StatementPattern(new Var("s"), new Var("p"), new Var("o")), new StatementPattern(new Var("x"), new Var("q"), new Var("o")));
		Union union2 = new Union(union1, new StatementPattern(new Var("a"), new Var("b"), new Var("o")));
		Filter filter = new Filter(union2, new Var("o"));
		Projection p = new Projection(filter);

		subject.meet(filter);

		assertThat(p.getArg()).isEqualTo(union2);
		assertThat(union2.getLeftArg()).isEqualTo(union1);
	}

	@Override
	public ExtendedQueryModelNormalizer getOptimizer() {
		return new ExtendedQueryModelNormalizer();
	}

}
