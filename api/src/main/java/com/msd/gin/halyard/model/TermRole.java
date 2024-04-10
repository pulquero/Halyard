/**
 * Copyright (c) 2016 Eclipse RDF4J contributors.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 */
package com.msd.gin.halyard.model;

import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;

public enum TermRole {
	SUBJECT {
		@Override
		public <E> E getValue(E s, E p, E o, E c) {
			return s;
		}
		@Override
		public Value getValue(Statement s) {
			return s.getSubject();
		}
		@Override
		public Var getVar(StatementPattern sp) {
			return sp.getSubjectVar();
		}
	},
	PREDICATE {
		@Override
		public <E> E getValue(E s, E p, E o, E c) {
			return p;
		}
		@Override
		public Value getValue(Statement s) {
			return s.getPredicate();
		}
		@Override
		public Var getVar(StatementPattern sp) {
			return sp.getPredicateVar();
		}
	},
	OBJECT {
		@Override
		public <E> E getValue(E s, E p, E o, E c) {
			return o;
		}
		@Override
		public Value getValue(Statement s) {
			return s.getObject();
		}
		@Override
		public Var getVar(StatementPattern sp) {
			return sp.getObjectVar();
		}
	},
	CONTEXT {
		@Override
		public <E> E getValue(E s, E p, E o, E c) {
			return c;
		}
		@Override
		public Value getValue(Statement s) {
			return s.getContext();
		}
		@Override
		public Var getVar(StatementPattern sp) {
			return sp.getContextVar();
		}
	};

	public abstract <E> E getValue(E s, E p, E o, E c);
	public abstract Value getValue(Statement s);
	public abstract Var getVar(StatementPattern sp);
}