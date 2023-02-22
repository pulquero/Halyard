package com.msd.gin.halyard.sail;

import com.google.common.cache.Cache;
import com.msd.gin.halyard.federation.HalyardFederatedService;
import com.msd.gin.halyard.federation.SailFederatedService;
import com.msd.gin.halyard.query.BindingSetPipe;

import java.util.Set;

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
	public void select(BindingSetPipe handler, Service service, Set<String> projectionVars, BindingSet bs, String baseUri) throws QueryEvaluationException {
		String queryString = service.getSelectQueryString(projectionVars);
		QueryBindingSet bindings = new QueryBindingSet(bs);
		sail.addQueryString(bindings, queryString);
		super.select(handler, service, projectionVars, bindings, baseUri);
	}

	@Override
	public FederatedService createPrivateInstance() {
		return new HBaseFederatedService(sail) {
			// NB: shared caches across all connections
			private final QueryCache queryCache = new QueryCache(sail.getConfiguration());
			private final Cache<IRI, Long> stmtCountCache = HalyardStatsBasedStatementPatternCardinalityCalculator.newStatementCountCache();

			@Override
			protected SailConnection getConnection() {
				return sail.getConnection(sail -> new HBaseSailConnection(sail, queryCache, stmtCountCache));
			}
		};
	}

}
