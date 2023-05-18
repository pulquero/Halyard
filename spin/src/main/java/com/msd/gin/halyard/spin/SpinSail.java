package com.msd.gin.halyard.spin;

import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolver;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunctionRegistry;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailException;

import com.msd.gin.halyard.algebra.evaluation.CloseableTripleSource;

/**
 * Interface for SAILs that support native SPIN evaluation.
 */
public interface SpinSail extends Sail {
	SpinParser getSpinParser();
	TupleFunctionRegistry getTupleFunctionRegistry();
	FederatedServiceResolver getFederatedServiceResolver();
	CloseableTripleSource newTripleSource() throws SailException;
}
