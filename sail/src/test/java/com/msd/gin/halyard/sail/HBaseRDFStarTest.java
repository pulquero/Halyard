package com.msd.gin.halyard.sail;

import com.msd.gin.halyard.common.HBaseServerTestInstance;
import com.msd.gin.halyard.repository.HBaseRepository;

import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.testsuite.repository.RDFStarSupportTest;

public class HBaseRDFStarTest extends RDFStarSupportTest {
	private static final int QUERY_TIMEOUT = 15;

	@Override
	protected Repository createRepository() {
		HBaseSail sail;
		try {
			sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "rdfstartable", true, 0, true, QUERY_TIMEOUT, null, null);
		} catch (Exception e) {
			throw new AssertionError(e);
		}
		Repository repo = new HBaseRepository(sail);
		repo.init();
		return repo;
	}

}
