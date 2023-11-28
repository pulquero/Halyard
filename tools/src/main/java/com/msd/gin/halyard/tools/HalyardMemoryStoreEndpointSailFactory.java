package com.msd.gin.halyard.tools;

import com.msd.gin.halyard.optimizers.HalyardEvaluationStatistics;
import com.msd.gin.halyard.optimizers.SimpleStatementPatternCardinalityCalculator;
import com.msd.gin.halyard.strategy.HalyardEvaluationExecutor;
import com.msd.gin.halyard.strategy.HalyardEvaluationStrategy;
import com.msd.gin.halyard.strategy.HalyardQueryOptimizerPipeline;
import com.msd.gin.halyard.strategy.StrategyConfig;
import com.msd.gin.halyard.tools.HalyardEndpoint.EndpointSailFactory;

import java.io.File;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.function.FunctionRegistry;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunctionRegistry;
import org.eclipse.rdf4j.query.parser.sparql.aggregate.CustomAggregateFunctionRegistry;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.eclipse.rdf4j.sail.memory.MemoryStoreConnection;

public class HalyardMemoryStoreEndpointSailFactory implements EndpointSailFactory {
	private Configuration conf;
	private File dataDir;

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	public void setDataDir(String s) {
		this.dataDir = new File(s);
	}

	@Override
	public HalyardMemoryStore createSail() {
		return (dataDir != null) ? new HalyardMemoryStore(conf, dataDir) : new HalyardMemoryStore(conf);
	}

	static final class HalyardMemoryStore extends MemoryStore {
		private final Configuration conf;

		public HalyardMemoryStore(Configuration conf) {
			this.conf = conf;
		}

		public HalyardMemoryStore(Configuration conf, File dataDir) {
			super(dataDir);
			this.conf = conf;
		}

		@Override
		protected NotifyingSailConnection getConnectionInternal() throws SailException {
			return new HalyardMemoryStoreConnection(this);
		}
	}

	static final class HalyardMemoryStoreConnection extends MemoryStoreConnection {
		private final Configuration conf;

		HalyardMemoryStoreConnection(HalyardMemoryStore sail) {
			super(sail);
			this.conf = sail.conf;
		}

		@Override
		protected EvaluationStrategy getEvaluationStrategy(Dataset dataset, final TripleSource tripleSource) {
			HalyardEvaluationStatistics stats = new HalyardEvaluationStatistics(SimpleStatementPatternCardinalityCalculator.FACTORY, null);
			HalyardEvaluationStrategy evalStrategy = new HalyardEvaluationStrategy(
				new StrategyConfig(conf),
				tripleSource,
				TupleFunctionRegistry.getInstance(),
				FunctionRegistry.getInstance(),
				CustomAggregateFunctionRegistry.getInstance(),
				dataset, getFederatedServiceResolver(),
				stats,
				HalyardEvaluationExecutor.create("Halyard", conf, false, Collections.emptyMap()));
			evalStrategy.setOptimizerPipeline(new HalyardQueryOptimizerPipeline(evalStrategy, tripleSource.getValueFactory(), stats));
			return evalStrategy;
		}
	}
}
