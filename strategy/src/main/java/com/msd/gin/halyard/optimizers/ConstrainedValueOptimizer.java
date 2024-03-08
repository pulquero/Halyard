package com.msd.gin.halyard.optimizers;

import com.msd.gin.halyard.query.algebra.Algebra;
import com.msd.gin.halyard.query.algebra.BGPCollector;
import com.msd.gin.halyard.query.algebra.ConstrainedStatementPattern;
import com.msd.gin.halyard.query.algebra.SkipVarsQueryModelVisitor;
import com.msd.gin.halyard.query.algebra.VarConstraint;
import com.msd.gin.halyard.query.algebra.evaluation.function.ParallelSplitFunction;
import com.msd.gin.halyard.common.RDFRole;
import com.msd.gin.halyard.common.StatementIndex;
import com.msd.gin.halyard.common.StatementIndices;
import com.msd.gin.halyard.common.ValueType;
import com.msd.gin.halyard.model.vocabulary.HALYARD;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.BooleanLiteral;
import org.eclipse.rdf4j.model.util.Literals;
import org.eclipse.rdf4j.model.vocabulary.AFN;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.Compare;
import org.eclipse.rdf4j.query.algebra.Compare.CompareOp;
import org.eclipse.rdf4j.query.algebra.Datatype;
import org.eclipse.rdf4j.query.algebra.Extension;
import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.FunctionCall;
import org.eclipse.rdf4j.query.algebra.IsBNode;
import org.eclipse.rdf4j.query.algebra.IsLiteral;
import org.eclipse.rdf4j.query.algebra.IsNumeric;
import org.eclipse.rdf4j.query.algebra.IsURI;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.Lang;
import org.eclipse.rdf4j.query.algebra.LeftJoin;
import org.eclipse.rdf4j.query.algebra.LocalName;
import org.eclipse.rdf4j.query.algebra.Namespace;
import org.eclipse.rdf4j.query.algebra.QueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TripleRef;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.UnaryValueOperator;
import org.eclipse.rdf4j.query.algebra.ValueConstant;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;

public class ConstrainedValueOptimizer implements QueryOptimizer {

	@Override
	public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
		tupleExpr.visit(new ConstraintScanner(dataset, bindings));
	}

	static final class ConstraintScanner extends SkipVarsQueryModelVisitor<RuntimeException> {
		final Dataset dataset;
		final BindingSet bindings;

		ConstraintScanner(Dataset dataset, BindingSet bindings) {
			this.dataset = dataset;
			this.bindings = bindings;
		}

		private void processGraphPattern(ConstraintCollector gpc) {
			for (StatementPattern sp: gpc.getStatementPatterns()) {
				Var svar = sp.getSubjectVar();
				Var pvar = sp.getPredicateVar();
				Var ovar = sp.getObjectVar();
				Var cvar = sp.getContextVar();
				Value s = Algebra.getVarValue(svar, bindings);
				Value p = Algebra.getVarValue(pvar, bindings);
				Value o = Algebra.getVarValue(ovar, bindings);
				Value c = Algebra.getVarValue(cvar, bindings);
				if (o == null) {
					Pair<VarConstraint,List<Filter>> constraintFilters = mergeConstraints(gpc.popConstraints(ovar));
					if (constraintFilters != null) {
						constrainStatementVar(ovar, RDFRole.Name.OBJECT, sp, s, p, o, c, constraintFilters);
						continue;
					}
				}
				if (s == null) {
					Pair<VarConstraint,List<Filter>> constraintFilters = mergeConstraints(gpc.popConstraints(svar));
					if (constraintFilters != null) {
						constrainStatementVar(svar, RDFRole.Name.SUBJECT, sp, s, p, o, c, constraintFilters);
						continue;
					}
				}
				if (p == null) {
					Pair<VarConstraint,List<Filter>> constraintFilters = mergeConstraints(gpc.popConstraints(pvar));
					if (constraintFilters != null) {
						constrainStatementVar(pvar, RDFRole.Name.PREDICATE, sp, s, p, o, c, constraintFilters);
						continue;
					}
				}
			}
		}

		private void constrainStatementVar(Var v, RDFRole.Name role, StatementPattern sp, Value s, Value p, Value o, Value c, Pair<VarConstraint,List<Filter>> constraintFilters) {
			if (sp instanceof ConstrainedStatementPattern) {
				throw new IllegalArgumentException("Statement pattern is already constrained");
			}
			boolean hasCtx = (c != null);
			if (!hasCtx && sp.getScope() == StatementPattern.Scope.DEFAULT_CONTEXTS) {
				hasCtx = (dataset != null) && !dataset.getDefaultGraphs().isEmpty();
			}
			VarConstraint constraint = constraintFilters.getLeft();
			StatementIndex.Name index;
			if (constraint.isPartitioned()) {
				// must provide index to use for partitioning
				index = StatementIndices.getIndexForConstraint(s != null, p != null, o != null, hasCtx, role);
			} else {
				index = null;
			}
			ConstrainedStatementPattern csp = new ConstrainedStatementPattern(sp, index, role, constraint);
			sp.replaceWith(csp);
			if (!constraintFilters.getRight().isEmpty()) {
				for (Filter f : constraintFilters.getRight()) {
					f.replaceWith(f.getArg());
				}
			}
		}

		private Pair<VarConstraint,List<Filter>> mergeConstraints(List<Pair<VarConstraint,Filter>> constraints) {
			if (constraints == null) {
				return null;
			}

			VarConstraint result = null;
			List<Filter> filters = new ArrayList<>(2);
			for (Pair<VarConstraint,Filter> p : constraints) {
				VarConstraint merged;
				if (result == null) {
					merged = p.getLeft();
				} else {
					merged = VarConstraint.merge(result, p.getLeft());
				}
				if (merged != null) {
					result = merged;
					Filter f = p.getRight();
					if (f != null) {
						filters.add(f);
					}
				}
			}
			return Pair.of(result, filters);
		}

		@Override
		public void meet(Filter filter) {
			ConstraintCollector collector = new ConstraintCollector(this, bindings);
			filter.visit(collector);
			processGraphPattern(collector);
		}

		@Override
		public void meet(Join join) {
			ConstraintCollector collector = new ConstraintCollector(this, bindings);
			join.visit(collector);
			processGraphPattern(collector);
		}
	}


	static final class ConstraintCollector extends BGPCollector<RuntimeException> {
		final Map<String,List<Pair<VarConstraint,Filter>>> varConstraints = new HashMap<>();
		final BindingSet bindings;

		ConstraintCollector(QueryModelVisitor<RuntimeException> visitor, BindingSet bindings) {
			super(visitor);
			this.bindings = bindings;
		}

		private void addConstraint(Var var, VarConstraint constraint, Filter filter) {
			varConstraints.computeIfAbsent(var.getName(), k -> new ArrayList<>(2)).add(Pair.of(constraint, filter));
		}

		List<Pair<VarConstraint,Filter>> popConstraints(Var var) {
			return varConstraints.remove(var.getName());
		}

		private void addExactConstraint(Var var, ValueType t, Filter filter) {
			// TODO: replace SPARQL Filter with RowFilter using BinaryComponentComparator then can pass down the filter for removal
			addConstraint(var, new VarConstraint(t), null/*filter*/);
		}

		private void addExactConstraint(Var var, int partitionCount, Filter filter) {
			addConstraint(var, VarConstraint.partitionConstraint(partitionCount), filter);
		}

		private void addBroadConstraint(Var var, ValueType t) {
			addConstraint(var, new VarConstraint(t), null);
		}

		private void addBroadConstraint(Var var, ValueType t, UnaryValueOperator func, CompareOp op, ValueExpr value) {
			addConstraint(var, new VarConstraint(t, func, op, value), null);
		}

		@Override
		public void meet(Filter filter) {
			ValueExpr condition = filter.getCondition();
			if (condition instanceof Compare) {
				Compare cmp = (Compare) filter.getCondition();
				ValueExpr leftArg = cmp.getLeftArg();
				CompareOp cmpOp = cmp.getOperator();
				if (leftArg instanceof UnaryValueOperator) {
					UnaryValueOperator func = (UnaryValueOperator) leftArg;
					if (isVar(func.getArg())) {
						Var var = (Var) func.getArg();
						if (func instanceof Datatype || func instanceof Lang) {
							addBroadConstraint(var, ValueType.LITERAL, func, cmpOp, cmp.getRightArg());
						} else if (cmpOp == CompareOp.EQ && BooleanLiteral.TRUE.equals(getValue(cmp.getRightArg()))) {
							if (func instanceof IsLiteral) {
								addExactConstraint(var, ValueType.LITERAL, filter);
							} else if (func instanceof IsURI) {
								addExactConstraint(var, ValueType.IRI, filter);
							} else if (func instanceof IsBNode) {
								addExactConstraint(var, ValueType.BNODE, filter);
							} else if (func instanceof IsNumeric) {
								addBroadConstraint(var, ValueType.LITERAL, func, CompareOp.EQ, cmp.getRightArg());
							}
						} else if (func instanceof LocalName) {
							// must be an IRI
							addBroadConstraint(var, ValueType.IRI);
						} else if (func instanceof Namespace) {
							// must be an IRI
							addBroadConstraint(var, ValueType.IRI, func, cmpOp, cmp.getRightArg());
						}
					}
				} else if (leftArg instanceof FunctionCall) {
					FunctionCall funcCall = (FunctionCall) leftArg;
					List<ValueExpr> args = funcCall.getArgs();
					if (AFN.LOCALNAME.stringValue().equals(funcCall.getURI()) && args.size() == 1 & isVar(args.get(0))) {
						// must be an IRI
						addBroadConstraint((Var) args.get(0), ValueType.IRI);
					}
				} else if (isVar(leftArg)) {
					if (cmpOp != CompareOp.NE && isLiteral(cmp.getRightArg())) {
						// var compared with a literal
						addBroadConstraint((Var) leftArg, ValueType.LITERAL);
					} else if (cmpOp == CompareOp.EQ && isIRI(cmp.getRightArg())) {
						// var compared with an IRI
						addBroadConstraint((Var) leftArg, ValueType.IRI);
					}
				}
			} else if (condition instanceof UnaryValueOperator) {
				UnaryValueOperator func = (UnaryValueOperator) condition;
				if (isVar(func.getArg())) {
					Var var = (Var) func.getArg();
					if (func instanceof IsLiteral) {
						// FILTER(IsLiteral(?x))
						addExactConstraint(var, ValueType.LITERAL, filter);
					} else if (func instanceof IsURI) {
						// FILTER(IsURI(?x))
						addExactConstraint(var, ValueType.IRI, filter);
					} else if (func instanceof IsBNode) {
						// FILTER(IsBNode(?x))
						addExactConstraint(var, ValueType.BNODE, filter);
					} else if (func instanceof IsNumeric) {
						// FILTER(IsNumeric(?x))
						addBroadConstraint(var, ValueType.LITERAL, func, CompareOp.EQ, new ValueConstant(BooleanLiteral.TRUE));
					}
				}
			} else if (condition instanceof FunctionCall) {
				FunctionCall funcCall = (FunctionCall) condition;
				List<ValueExpr> args = funcCall.getArgs();
				if (HALYARD.PARALLEL_SPLIT_FUNCTION.stringValue().equals(funcCall.getURI()) && args.size() == 2 && args.get(0) instanceof ValueConstant && isVar(args.get(1))) {
					int partitionCount = Literals.getIntValue(((ValueConstant) args.get(0)).getValue(), -1);
					if (partitionCount > 1) {
						addExactConstraint((Var) args.get(1), ParallelSplitFunction.toActualForkCount(partitionCount), filter);
					}
				}
			}

			filter.getArg().visit(this);
		}

		@Override
		public void meet(LeftJoin leftJoin) {
			leftJoin.getLeftArg().visit(this);
		}

		@Override
		public void meet(Extension node) {
			node.getArg().visit(this);
		}

		@Override
		public void meet(TripleRef tripleRef) {
			addBroadConstraint(tripleRef.getExprVar(), ValueType.TRIPLE);
		}

		private Value getValue(ValueExpr expr) {
			if (expr instanceof ValueConstant) {
				return ((ValueConstant) expr).getValue();
			} else if (isVar(expr)) {
				return Algebra.getVarValue((Var) expr, bindings);
			} else {
				return null;
			}
		}

		private boolean isLiteral(ValueExpr expr) {
			Value v = getValue(expr);
			return (v != null) && v.isLiteral();
		}

		private boolean isIRI(ValueExpr expr) {
			Value v = getValue(expr);
			return (v != null) && v.isIRI();
		}
	}

	private static boolean isVar(ValueExpr expr) {
		return (expr instanceof Var);
	}
}
