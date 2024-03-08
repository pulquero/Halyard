package com.msd.gin.halyard.function;

import com.msd.gin.halyard.model.vocabulary.HALYARD;
import com.msd.gin.halyard.spin.function.InverseMagicProperty;

import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunction;
import org.kohsuke.MetaInfServices;

/**
 * The reverse predicate of {@link IdentifierTupleFunction}.
 */
@MetaInfServices(TupleFunction.class)
public class ValueTupleFunction extends IdentifierTupleFunction implements InverseMagicProperty {

	@Override
	public String getURI() {
		return HALYARD.VALUE_PROPERTY.stringValue();
	}
}
