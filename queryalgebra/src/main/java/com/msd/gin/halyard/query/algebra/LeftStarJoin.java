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
package com.msd.gin.halyard.query.algebra;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.QueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.StatementPattern.Scope;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Var;

/**
 * Collection of left joins.
 */
public class LeftStarJoin extends NAryTupleOperator {
	private static final long serialVersionUID = -2918969614574374511L;

	private StatementPattern.Scope scope;
	private Var commonVar;
	private Var contextVar;

	public LeftStarJoin(Var commonVar, @Nullable Var contextVar, TupleExpr base, List<StatementPattern> stmts) {
		this(StatementPattern.Scope.DEFAULT_CONTEXTS, commonVar, contextVar, base, stmts);
	}

	public LeftStarJoin(StatementPattern.Scope scope, Var commonVar, @Nullable Var contextVar, TupleExpr base, List<StatementPattern> stmts) {
		this.scope = scope;
		setCommonVar(commonVar);
		setContextVar(contextVar);
		List<TupleExpr> exprs = new ArrayList<>(1 + stmts.size());
		exprs.add(base);
		exprs.addAll(stmts);
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

	public TupleExpr getBaseArg() {
		return getArg(0);
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
		if (other instanceof LeftStarJoin) {
			LeftStarJoin o = (LeftStarJoin) other;
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
	public LeftStarJoin clone() {
		LeftStarJoin clone = (LeftStarJoin) super.clone();

		clone.setCommonVar(commonVar.clone());
		if (contextVar != null) {
			clone.setContextVar(contextVar.clone());
		}

		return clone;
	}
}
