package com.msd.gin.halyard.optimizers;

import com.msd.gin.halyard.query.algebra.AbstractExtendedQueryModelVisitor;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.BindingSetAssignment;
import org.eclipse.rdf4j.query.algebra.Extension;
import org.eclipse.rdf4j.query.algebra.ExtensionElem;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.SingletonSet;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.ValueConstant;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;

public final class ExtendedBindingAssignerOptimizer implements QueryOptimizer {

	@Override
	public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
		if (!bindings.isEmpty()) {
			tupleExpr.visit(new VarVisitor(bindings));
		}
	}

	private static class VarVisitor extends AbstractExtendedQueryModelVisitor<RuntimeException> {

		private final BindingSet bindings;

		public VarVisitor(BindingSet bindings) {
			this.bindings = bindings;
		}

		@Override
		public void meet(Var var) {
			if (!var.hasValue()) {
				String name = var.getName();
				Value value = bindings.getValue(name);
				if (value != null) {
					Var replacement = new Var(name, value, var.isAnonymous(), var.isConstant());
					var.replaceWith(replacement);
				}
			}
		}

		@Override
		public void meet(BindingSetAssignment bsa) {
			TupleExpr ext = null;
			for (String name : bsa.getBindingNames()) {
				Value value = bindings.getValue(name);
				if (value != null) {
					if (ext == null) {
						ext = new SingletonSet();
					}
					ext = new Extension(ext, new ExtensionElem(new ValueConstant(value), name));
				}
			}
			if (ext != null) {
				Join newJoin = new Join();
				bsa.replaceWith(newJoin);
				newJoin.setLeftArg(ext);
				newJoin.setRightArg(bsa);
			}
		}
	}
}
