package com.msd.gin.halyard.optimizers;

import org.eclipse.rdf4j.query.algebra.BindingSetAssignment;
import org.eclipse.rdf4j.query.algebra.Extension;
import org.eclipse.rdf4j.query.algebra.Projection;
import org.eclipse.rdf4j.query.algebra.SingletonSet;
import org.eclipse.rdf4j.query.algebra.Union;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ExtendedUnionScopeChangeOptimizerTest extends AbstractOptimizerTest {

	private ExtendedUnionScopeChangeOptimizer subject;
	private Union union;

	@BeforeEach
	public void setup() throws Exception {
		subject = getOptimizer();
		union = new Union();
		union.setVariableScopeChange(true);

	}

	@Test
	public void fixesScopeChange() {
		union.setRightArg(new SingletonSet());
		union.setLeftArg(new SingletonSet());

		subject.optimize(union, null, null);
		assertThat(union.isVariableScopeChange()).isFalse();
	}

	@Test
	public void keepsScopeChangeOnBindClauseArg() {
		{
			union.setLeftArg(new Extension(new SingletonSet()));
			union.setRightArg(new SingletonSet());

			subject.optimize(union, null, null);
			assertThat(union.isVariableScopeChange()).isTrue();
		}

		{
			union.setLeftArg(new SingletonSet());
			union.setRightArg(new Extension(new SingletonSet()));

			subject.optimize(union, null, null);
			assertThat(union.isVariableScopeChange()).isTrue();
		}
	}

	@Test
	public void fixesScopeChangeOnSubselect1() {
		union.setLeftArg(new Projection(new Extension(new SingletonSet())));
		union.setRightArg(new SingletonSet());

		subject.optimize(union, null, null);
		assertThat(union.isVariableScopeChange()).isFalse();
	}

	@Test
	public void fixesScopeChangeOnSubselect2() {
		union.setLeftArg(new SingletonSet());
		union.setRightArg(new Projection(new Extension(new SingletonSet())));

		subject.optimize(union, null, null);
		assertThat(union.isVariableScopeChange()).isFalse();
	}

	@Test
	public void fixesScopeChangeOnNestedUnion() {
		Union nestedUnion = new Union();
		nestedUnion.setVariableScopeChange(true);
		nestedUnion.setLeftArg(new Extension(new SingletonSet()));
		nestedUnion.setRightArg(new SingletonSet());
		{
			union.setLeftArg(nestedUnion);
			union.setRightArg(new SingletonSet());

			subject.optimize(union, null, null);
			assertThat(union.isVariableScopeChange()).isFalse();
		}

		{
			union.setLeftArg(new SingletonSet());
			union.setRightArg(nestedUnion);

			subject.optimize(union, null, null);
			assertThat(union.isVariableScopeChange()).isFalse();
		}

	}

	public void keepsScopeChangeOnNestedPathAlternative() {
		Union nestedUnion = new Union();
		nestedUnion.setVariableScopeChange(false);
		nestedUnion.setLeftArg(new Extension(new SingletonSet()));
		nestedUnion.setRightArg(new SingletonSet());

		union.setLeftArg(new SingletonSet());
		union.setRightArg(nestedUnion);

		subject.optimize(union, null, null);
		assertThat(union.isVariableScopeChange()).isTrue();
	}

	@Test
	public void keepsScopeChangeOnValuesClauseArg() {
		{
			union.setLeftArg(new BindingSetAssignment());
			union.setRightArg(new SingletonSet());

			subject.optimize(union, null, null);
			assertThat(union.isVariableScopeChange()).isTrue();
		}

		{
			union.setLeftArg(new SingletonSet());
			union.setRightArg(new BindingSetAssignment());

			subject.optimize(union, null, null);
			assertThat(union.isVariableScopeChange()).isTrue();
		}
	}

	@Override
	public ExtendedUnionScopeChangeOptimizer getOptimizer() {
		return new ExtendedUnionScopeChangeOptimizer();
	}

}
