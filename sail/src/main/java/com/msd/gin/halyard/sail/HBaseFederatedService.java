package com.msd.gin.halyard.sail;

import com.msd.gin.halyard.federation.HalyardFederatedService;
import com.msd.gin.halyard.federation.SailFederatedService;
import com.msd.gin.halyard.strategy.HalyardEvaluationStrategy;

import java.util.Set;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.MutableBindingSet;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;
import org.eclipse.rdf4j.sail.SailConnection;

public class HBaseFederatedService extends SailFederatedService implements HalyardFederatedService {
	private final HBaseSail sail;

	public HBaseFederatedService(HBaseSail sail) {
		super(sail);
		this.sail = sail;
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
		sail.addQueryString(bs, queryString);
		return bs;
	}

	@Override
	public FederatedService createEvaluationInstance(HalyardEvaluationStrategy strategy) {
		return new HBaseFederatedService(sail) {
			@Override
			protected SailConnection getConnection() {
				return sail.getConnection(sail -> {
					HBaseSailConnection conn = new HBaseSailConnection(sail, strategy.getExecutor());
					conn.setTrackResultSize(strategy.isTrackResultSize());
					conn.setTrackResultTime(strategy.isTrackTime());
					return conn;
				});
			}
		};
	}

}
