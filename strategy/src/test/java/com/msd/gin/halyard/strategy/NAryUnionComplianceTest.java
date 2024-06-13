package com.msd.gin.halyard.strategy;

import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.config.RepositoryConfigException;
import org.eclipse.rdf4j.repository.config.RepositoryImplConfig;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.config.SailRepositoryFactory;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.testsuite.sparql.RepositorySPARQLComplianceTestSuite;

public class NAryUnionComplianceTest extends RepositorySPARQLComplianceTestSuite {

	public static SailRepositoryFactory createFactory() {
		return new SailRepositoryFactory() {
			@Override
			public Repository getRepository(RepositoryImplConfig config) throws RepositoryConfigException {
				Sail sail = new MockSailWithHalyardStrategy(0, 0, Float.MAX_VALUE, Integer.MAX_VALUE, 1, 0);
				return new SailRepository(sail);
			}
		};
	}

	public NAryUnionComplianceTest() {
		super(createFactory());
	}
}
