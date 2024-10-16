package com.msd.gin.halyard.repository;

import com.msd.gin.halyard.sail.HBaseSail;
import com.msd.gin.halyard.sail.HBaseSailConnection;

import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.SailException;

public class HBaseRepository extends SailRepository {

	public HBaseRepository(HBaseSail sail) {
		super(sail);
	}

	@Override
	public HBaseRepositoryConnection getConnection() throws RepositoryException {
		if (!isInitialized()) {
			init();
		}
		try {
			return new HBaseRepositoryConnection(this, (HBaseSailConnection) getSail().getConnection());
		} catch (SailException e) {
			throw new RepositoryException(e);
		}
	}

}
