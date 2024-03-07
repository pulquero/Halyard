package com.msd.gin.halyard.sail;

import com.msd.gin.halyard.federation.HalyardFederatedService;
import com.msd.gin.halyard.federation.SailFederatedService;
import com.msd.gin.halyard.strategy.HalyardEvaluationStrategy;

import java.util.Set;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.MutableBindingSet;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;
import org.eclipse.rdf4j.sail.SailConnection;

public class HBaseFederatedService extends SailFederatedService implements HalyardFederatedService {
	private final HBaseSail sail;
	private final int forkIndex;
	private final int forkCount;

	HBaseFederatedService(HBaseSail sail) {
		this(sail, -1, 0);
	}

	private HBaseFederatedService(HBaseSail sail, int forkIndex, int forkCount) {
		super(sail);
		this.sail = sail;
		this.forkIndex = forkIndex;
		this.forkCount = forkCount;
	}

	@Override
	protected MutableBindingSet createBindings(Service service, Set<String> projectionVars, BindingSet bindings) {
		MutableBindingSet bs = super.createBindings(service, projectionVars, bindings);
		String queryString;
		if (projectionVars != null) {
			queryString = service.getSelectQueryString(projectionVars);
		} else {
			queryString = service.getAskQueryString();
		}
		ValueFactory vf = sail.getValueFactory();
		bs.setBinding(HBaseSailConnection.SOURCE_STRING_BINDING, vf.createLiteral(queryString));
		if (forkCount > 1) {
			bs.setBinding(HBaseSailConnection.FORK_INDEX_BINDING, vf.createLiteral(forkIndex));
			bs.setBinding(HBaseSailConnection.FORK_COUNT_BINDING, vf.createLiteral(forkCount));
		}
		return bs;
	}

	@Override
	public FederatedService createEvaluationInstance(HalyardEvaluationStrategy strategy, int forkIndex, int forkCount) {
		return new HBaseFederatedService(sail, forkIndex, forkCount) {
			@Override
			protected SailConnection getConnection() {
				return sail.getConnection(sail -> {
					HBaseSailConnection conn = new HBaseSailConnection(sail, strategy.getExecutor());
					conn.setTrackResultSize(strategy.isTrackResultSize());
					conn.setTrackResultTime(strategy.isTrackTime());
					conn.setTrackBranchOperatorsOnly(strategy.isTrackBranchOperatorsOnly());
					return conn;
				});
			}
		};
	}

}
