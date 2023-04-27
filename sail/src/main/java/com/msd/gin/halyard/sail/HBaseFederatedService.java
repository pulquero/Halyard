package com.msd.gin.halyard.sail;

import com.google.common.cache.Cache;
import com.msd.gin.halyard.federation.HalyardFederatedService;
import com.msd.gin.halyard.federation.SailFederatedService;
import com.msd.gin.halyard.strategy.HalyardEvaluationStrategy;

import java.util.Set;
import java.util.function.Consumer;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;
import org.eclipse.rdf4j.sail.SailConnection;

public class HBaseFederatedService extends SailFederatedService implements HalyardFederatedService {
	private final HBaseSail sail;

	public HBaseFederatedService(HBaseSail sail) {
		super(sail);
		this.sail = sail;
	}

	@Override
	public boolean ask(Service service, BindingSet bs, String baseUri) throws QueryEvaluationException {
		String queryString = service.getAskQueryString();
		QueryBindingSet bindings = new QueryBindingSet(bs);
		sail.addQueryString(bindings, queryString);
		return super.ask(service, bindings, baseUri);
	}

	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> select(Service service, Set<String> projectionVars, BindingSet bs, String baseUri) throws QueryEvaluationException {
		String queryString = service.getSelectQueryString(projectionVars);
		QueryBindingSet bindings = new QueryBindingSet(bs);
		sail.addQueryString(bindings, queryString);
		return super.select(service, projectionVars, bindings, baseUri);
	}

	@Override
	public void select(Consumer<BindingSet> handler, Service service, Set<String> projectionVars, BindingSet bs, String baseUri) throws QueryEvaluationException {
		String queryString = service.getSelectQueryString(projectionVars);
		QueryBindingSet bindings = new QueryBindingSet(bs);
		sail.addQueryString(bindings, queryString);
		super.select(handler, service, projectionVars, bindings, baseUri);
	}

	@Override
	public FederatedService createPrivateInstance(HalyardEvaluationStrategy strategy) {
		return new HBaseFederatedService(sail) {
			// NB: shared caches across all connections
			private final QueryCache queryCache = new QueryCache(sail.queryCacheSize);
			private final Cache<IRI, Long> stmtCountCache = HalyardStatsBasedStatementPatternCardinalityCalculator.newStatementCountCache();

			@Override
			protected SailConnection getConnection() {
				return sail.getConnection(sail -> {
					HBaseSailConnection conn = new HBaseSailConnection(sail, queryCache, stmtCountCache, strategy.getExecutor());
					conn.setTrackResultSize(strategy.isTrackResultSize());
					conn.setTrackResultTime(strategy.isTrackTime());
					return conn;
				});
			}
		};
	}

}
