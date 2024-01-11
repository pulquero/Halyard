package com.msd.gin.halyard.algebra;

import com.msd.gin.halyard.common.RDFRole;
import com.msd.gin.halyard.common.StatementIndex;

import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;

public final class ConstrainedStatementPattern extends StatementPattern {

	private static final long serialVersionUID = -1551292826547140642L;

	private StatementIndex.Name indexToUse;
	private RDFRole.Name constrainedRole;
	private VarConstraint constraint;

	public ConstrainedStatementPattern(StatementPattern sp, StatementIndex.Name indexToUse, RDFRole.Name constrainedRole, VarConstraint constraint) {
		this(sp.getScope(), sp.getSubjectVar().clone(), sp.getPredicateVar().clone(), sp.getObjectVar().clone(), sp.getContextVar() != null ? sp.getContextVar().clone() : null, indexToUse, constrainedRole, constraint);
	}

	public ConstrainedStatementPattern(Scope scope, Var subject, Var predicate, Var object, Var context, StatementIndex.Name indexToUse, RDFRole.Name constrainedRole, VarConstraint constraint) {
		super(scope, subject, predicate, object, context);
		this.indexToUse = indexToUse;
		this.constrainedRole = constrainedRole;
		this.constraint = constraint;
	}

	public StatementIndex.Name getIndexToUse() {
		return indexToUse;
	}

	public RDFRole.Name getConstrainedRole() {
		return constrainedRole;
	}

	public VarConstraint getConstraint() {
		return constraint;
	}

	@Override
	public String getSignature() {
		Var constrainedVar = constrainedRole.getValue(this.getSubjectVar(), this.getPredicateVar(), this.getObjectVar(), this.getContextVar());
		return super.getSignature() + " [" + indexToUse + " ?" + constrainedVar.getName() + " is " + constraint + "]";
	}

	@Override
	public boolean equals(Object other) {
		if (other instanceof ConstrainedStatementPattern) {
			ConstrainedStatementPattern o = (ConstrainedStatementPattern) other;
			return super.equals(other)
					&& this.indexToUse == o.indexToUse
					&& this.constrainedRole == o.constrainedRole
					&& this.constraint.equals(o.constraint);
		}
		return false;
	}

	@Override
	public int hashCode() {
		int result = super.hashCode();
		result = 89 * result + indexToUse.hashCode();
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
