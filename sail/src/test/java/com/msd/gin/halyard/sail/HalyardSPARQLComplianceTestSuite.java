package com.msd.gin.halyard.sail;

import com.msd.gin.halyard.common.HBaseServerTestInstance;
import com.msd.gin.halyard.repository.HBaseRepositoryConfig;

import org.eclipse.rdf4j.repository.config.RepositoryImplConfig;
import org.eclipse.rdf4j.repository.sail.config.SailRepositoryFactory;
import org.eclipse.rdf4j.testsuite.sparql.RepositorySPARQLComplianceTestSuite;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class HalyardSPARQLComplianceTestSuite extends RepositorySPARQLComplianceTestSuite {

	@BeforeClass
	public static void setUpFactory() throws Exception {
		HBaseServerTestInstance.getInstanceConfig();
		setRepositoryFactory(new SailRepositoryFactory() {
			@Override
			public RepositoryImplConfig getConfig() {
				HBaseSailConfig sailConfig = new HBaseSailConfig();
				sailConfig.setCreate(true);
				sailConfig.setPush(true);
				sailConfig.setTableName("complianceTestSuite");
				return new HBaseRepositoryConfig(sailConfig);
			}
		});
	}

	@AfterClass
	public static void tearDownFactory() throws Exception {
		setRepositoryFactory(null);
	}
}
