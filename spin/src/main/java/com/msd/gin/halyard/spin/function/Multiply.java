/*******************************************************************************
 * Copyright (c) 2015 Eclipse RDF4J contributors, Aduna, and others.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 *******************************************************************************/
package com.msd.gin.halyard.spin.function;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.SP;
import org.eclipse.rdf4j.query.algebra.MathExpr.MathOp;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.BinaryFunction;

import com.msd.gin.halyard.strategy.MathOpEvaluator;

public class Multiply extends BinaryFunction {

	@Override
	public String getURI() {
		return SP.MUL.toString();
	}

	@Override
	protected Value evaluate(ValueFactory valueFactory, Value arg1, Value arg2) throws ValueExprEvaluationException {
		if (arg1.isLiteral() && arg2.isLiteral()) {
			return new MathOpEvaluator().evaluate((Literal) arg1, (Literal) arg2, MathOp.MULTIPLY, valueFactory);
		}

		throw new ValueExprEvaluationException("Both arguments must be numeric literals");
	}
}
