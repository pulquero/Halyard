package com.msd.gin.halyard.query.algebra;

import com.msd.gin.halyard.common.StatementIndex;
import com.msd.gin.halyard.model.TermRole;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;

public final class ConstrainedStatementPattern extends StatementPattern {

	private static final long serialVersionUID = -1551292826547140642L;

	private @Nullable StatementIndex.Name indexToPartition;
	private @Nonnull TermRole constrainedRole;
	private @Nonnull VarConstraint constraint;

	public ConstrainedStatementPattern(StatementPattern sp, StatementIndex.Name indexToPartition, TermRole constrainedRole, VarConstraint constraint) {
		this(sp.getScope(), sp.getSubjectVar().clone(), sp.getPredicateVar().clone(), sp.getObjectVar().clone(), sp.getContextVar() != null ? sp.getContextVar().clone() : null, indexToPartition, constrainedRole, constraint);
	}

	public ConstrainedStatementPattern(Scope scope, Var subject, Var predicate, Var object, Var context, StatementIndex.Name indexToPartition, TermRole constrainedRole, VarConstraint constraint) {
		super(scope, subject, predicate, object, context);
		if (constraint.isPartitioned() && indexToPartition == null) {
			throw new IllegalArgumentException("Index to partition must be specified");
		} else if (!constraint.isPartitioned() && indexToPartition != null) {
			throw new IllegalArgumentException("No partitioning specified");
		}
		this.indexToPartition = indexToPartition;
		this.constrainedRole = constrainedRole;
		this.constraint = constraint;
	}

	public StatementIndex.Name getIndexToPartition() {
		return indexToPartition;
	}

	public TermRole getConstrainedRole() {
		return constrainedRole;
	}

	public Var getConstrainedVar() {
		return constrainedRole.getVar(this);
	}

	public VarConstraint getConstraint() {
		return constraint;
	}

	@Override
	public String getSignature() {
		return super.getSignature() + " [" + (indexToPartition!=null ? indexToPartition+" " : "") + "?" + getConstrainedVar().getName() + " is " + constraint + "]";
	}

	@Override
	public boolean equals(Object other) {
		if (other instanceof ConstrainedStatementPattern) {
			ConstrainedStatementPattern o = (ConstrainedStatementPattern) other;
			return super.equals(other)
					&& this.indexToPartition == o.indexToPartition
					&& this.constrainedRole == o.constrainedRole
					&& this.constraint.equals(o.constraint);
		}
		return false;
	}

	@Override
	public int hashCode() {
		int result = super.hashCode();
		result = 89 * result + Objects.hashCode(indexToPartition);
		result = 89 * result + constrainedRole.hashCode();
		result = 89 * result + constraint.hashCode();
		return result;
	}

	@Override
	public ConstrainedStatementPattern clone() {
		ConstrainedStatementPattern clone = (ConstrainedStatementPattern) super.clone();
		if (constraint != null) {
			clone.constraint = constraint.clone();
		}
		return clone;
	}
}
