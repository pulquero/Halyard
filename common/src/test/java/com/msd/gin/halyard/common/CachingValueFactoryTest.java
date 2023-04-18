package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.ValueFactoryTest;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

public class CachingValueFactoryTest extends ValueFactoryTest {

	@Override
	protected ValueFactory factory() {
		return new CachingValueFactory(SimpleValueFactory.getInstance(), 1);
	}

}
