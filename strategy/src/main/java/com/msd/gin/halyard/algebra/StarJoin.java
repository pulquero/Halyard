/*
 * Copyright 2018 Merck Sharp & Dohme Corp. a subsidiary of Merck & Co.,
 * Inc., Kenilworth, NJ, USA.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.msd.gin.halyard.algebra;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.QueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.StatementPattern.Scope;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.helpers.collectors.StatementPatternCollector;

/**
 * Collection of joins (incl. filters) that share a common subject var and context var (if present), e.g. ?s :p1 ?o1; :p2 ?o2; :p3 ?o3.
 * In some cases, it is faster to evaluate these as ?s ?p ?o and then filter the results (?s can be determined at evaluation time, either from the available bindings or by evaluating the first argument).
 */
public class StarJoin extends NAryTupleOperator {
	private static final long serialVersionUID = -4523270958311045771L;

	private StatementPattern.Scope scope;
	private Var commonVar;
	private Var contextVar;

	public StarJoin(Var commonVar, @Nullable Var contextVar, List<StatementPattern> exprs) {
		this(StatementPattern.Scope.DEFAULT_CONTEXTS, commonVar, contextVar, exprs);
	}

	public StarJoin(StatementPattern.Scope scope, Var commonVar, @Nullable Var contextVar, List<StatementPattern> exprs) {
		assert exprs.size() > 1;
		this.scope = scope;
		setCommonVar(commonVar);
		setContextVar(contextVar);
		setArgs(exprs);
	}

	public StatementPattern.Scope getScope() {
		return scope;
	}

	private void setCommonVar(Var var) {
		var.setParentNode(this);
		commonVar = var;
	}

	public Var getCommonVar() {
		return commonVar;
	}

	private void setContextVar(@Nullable Var var) {
		if (var != null) {
			var.setParentNode(this);
		}
		contextVar = var;
	}

	public @Nullable Var getContextVar() {
		return contextVar;
	}

	public List<Var> getVarList() {
		return getVars(new ArrayList<>(4*getArgCount()));
	}

	public <L extends Collection<Var>> L getVars(L varCollection) {
		StatementPatternCollector spc = new StatementPatternCollector();
		for(TupleExpr expr : getArgs()) {
			expr.visit(spc);
		}
		for(StatementPattern sp : spc.getStatementPatterns()) {
			sp.getVars(varCollection);
		}
		return varCollection;
	}

	@Override
	public <X extends Exception> void visitChildren(final QueryModelVisitor<X> visitor) throws X {
		commonVar.visit(visitor);
		if (contextVar != null) {
			contextVar.visit(visitor);
		}
		super.visitChildren(visitor);
	}

	@Override
	public void replaceChildNode(final QueryModelNode current, final QueryModelNode replacement) {
		if (current == commonVar) {
			setCommonVar((Var) replacement);
		} else if (current == contextVar) {
			setContextVar((Var) replacement);
		} else {
			super.replaceChildNode(current, replacement);
		}
	}

	@Override
	public String getSignature() {
		StringBuilder sb = new StringBuilder(128);
		sb.append(super.getSignature());
		if (scope == Scope.NAMED_CONTEXTS) {
			sb.append(" FROM NAMED CONTEXT");
		}
		return sb.toString();
	}

	@Override
	public boolean equals(Object other) {
		if (other instanceof StarJoin) {
			StarJoin o = (StarJoin) other;
			return commonVar.equals(o.commonVar)
					&& Objects.equals(contextVar, o.contextVar)
					&& scope.equals(o.getScope())
					&& super.equals(other);
		}
		return false;
	}

	@Override
	public int hashCode() {
		return Objects.hash(commonVar, contextVar, scope, super.hashCode());
	}

	@Override
	public StarJoin clone() {
		StarJoin clone = (StarJoin) super.clone();

		clone.setCommonVar(commonVar.clone());
		if (contextVar != null) {
			clone.setContextVar(contextVar.clone());
		}

		return clone;
	}
}
