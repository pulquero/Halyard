package com.msd.gin.halyard.sail;

import com.msd.gin.halyard.federation.HalyardFederatedService;
import com.msd.gin.halyard.federation.SailFederatedService;
import com.msd.gin.halyard.function.ParallelSplitFunction;
import com.msd.gin.halyard.strategy.HalyardEvaluationContext;
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

	public HBaseFederatedService(HBaseSail sail, int forkIndex) {
		super(sail);
		this.sail = sail;
		this.forkIndex = forkIndex;
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
		if (forkIndex != ParallelSplitFunction.NO_FORKING) {
			bs.setBinding(HBaseSailConnection.FORK_INDEX_BINDING, vf.createLiteral(forkIndex));
		}
		return bs;
	}

	@Override
	public FederatedService createEvaluationInstance(HalyardEvaluationStrategy strategy, HalyardEvaluationContext evalContext) {
		return new HBaseFederatedService(sail, evalContext.getForkIndex()) {
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
