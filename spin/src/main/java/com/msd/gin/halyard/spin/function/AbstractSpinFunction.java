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
import org.eclipse.rdf4j.query.Query;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;

public abstract class AbstractSpinFunction {

	private final String uri;

	protected AbstractSpinFunction(String uri) {
		this.uri = uri;
	}

	public String getURI() {
		return uri;
	}

	protected static void addBindings(Query query, Value... args) throws ValueExprEvaluationException {
		for (int i = 1; i < args.length; i += 2) {
			if (!(args[i] instanceof Literal)) {
				throw new ValueExprEvaluationException("Argument " + i + " must be a literal");
			}
			query.setBinding(((Literal) args[i]).getLabel(), args[i + 1]);
		}
	}
}
