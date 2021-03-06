package com.msd.gin.halyard.sail;

import com.msd.gin.halyard.common.HBaseServerTestInstance;

import org.eclipse.rdf4j.repository.RDFStarSupportTest;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.Sail;

public class HBaseRDFStarTest extends RDFStarSupportTest {

	@Override
	protected Repository createRepository() {
		Sail sail;
		try {
			sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertable", true, 0, true, 0, null, null);
		} catch (Exception e) {
			throw new AssertionError(e);
		}
		Repository repo = new SailRepository(sail);
		repo.init();
		return repo;
	}

}
