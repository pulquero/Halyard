package com.msd.gin.halyard.repository;

import com.msd.gin.halyard.common.HBaseServerTestInstance;
import com.msd.gin.halyard.sail.HBaseSail;
import com.msd.gin.halyard.strategy.StrategyConfig;

import org.apache.hadoop.conf.Configuration;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.config.RepositoryConfigException;
import org.eclipse.rdf4j.repository.config.RepositoryImplConfig;
import org.eclipse.rdf4j.testsuite.sparql.RepositorySPARQLComplianceTestSuite;

public class HBaseSPARQLComplianceTest extends RepositorySPARQLComplianceTestSuite {
	private static final int QUERY_TIMEOUT = 15;

	public static HBaseRepositoryFactory createFactory() {
		Configuration conf;
		try {
			conf = HBaseServerTestInstance.getInstanceConfig();
		} catch (Exception e) {
			throw new AssertionError(e);
		}
		conf.setInt(StrategyConfig.HALYARD_EVALUATION_THREADS, 5);
		return new HBaseRepositoryFactory() {
			@Override
			public Repository getRepository(RepositoryImplConfig config) throws RepositoryConfigException {
				HBaseSail sail = new HBaseSail(conf, "complianceTestSuite", true, 0, true, QUERY_TIMEOUT, null, null);
				return new HBaseRepository(sail);
			}
		};
	}

	public HBaseSPARQLComplianceTest() {
		super(createFactory());
	}
}
