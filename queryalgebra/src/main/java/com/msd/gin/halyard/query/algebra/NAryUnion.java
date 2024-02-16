package com.msd.gin.halyard.query.algebra;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.eclipse.rdf4j.query.algebra.TupleExpr;

public class NAryUnion extends NAryTupleOperator {
	private static final long serialVersionUID = 3627321682402493645L;

	public NAryUnion(List<TupleExpr> exprs) {
		assert exprs.size() > 1;
		setArgs(exprs);
	}

	@Override
	public Set<String> getAssuredBindingNames() {
		Set<String> bindingNames = new LinkedHashSet<>(16);
		bindingNames.addAll(getArg(0).getAssuredBindingNames());
		for (int i=1; i<getArgCount(); i++) {
			bindingNames.retainAll(getArg(i).getAssuredBindingNames());
		}
		return bindingNames;
	}

	@Override
	public boolean equals(Object other) {
		if (other instanceof NAryUnion) {
			NAryUnion o = (NAryUnion) other;
			return super.equals(other);
		}
		return false;
	}

	@Override
	public int hashCode() {
		return super.hashCode() ^ "NAryUnion".hashCode();
	}

	@Override
	public NAryUnion clone() {
		return (NAryUnion) super.clone();
	}
}
