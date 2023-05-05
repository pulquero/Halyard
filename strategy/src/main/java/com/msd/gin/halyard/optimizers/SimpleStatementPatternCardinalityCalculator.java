package com.msd.gin.halyard.optimizers;

import java.io.IOException;
import java.util.Collection;

import org.eclipse.rdf4j.query.algebra.Var;

public class SimpleStatementPatternCardinalityCalculator implements StatementPatternCardinalityCalculator {
	public static final StatementPatternCardinalityCalculator.Factory FACTORY = () -> new SimpleStatementPatternCardinalityCalculator();

	public static final double SUBJECT_VAR_CARDINALITY = 1000.0;
	public static final double PREDICATE_VAR_CARDINALITY = 10.0;
	public static final double OBJECT_VAR_CARDINALITY = 1000.0;
	public static final double CONTEXT_VAR_CARDINALITY = 10.0;

	@Override
	public double getStatementCardinality(Var subjVar, Var predVar, Var objVar, Var ctxVar, Collection<String> boundVars) {
		return getSubjectCardinality(subjVar, boundVars) * getPredicateCardinality(predVar, boundVars) * getObjectCardinality(objVar, boundVars) * getContextCardinality(ctxVar, boundVars);
	}

	@Override
	public double getTripleCardinality(Var subjVar, Var predVar, Var objVar, Collection<String> boundVars) {
		return getSubjectCardinality(subjVar, boundVars) * getPredicateCardinality(predVar, boundVars) * getObjectCardinality(objVar, boundVars);
	}

	protected double getSubjectCardinality(Var var, Collection<String> boundVars) {
		return getCardinality(var, boundVars, SUBJECT_VAR_CARDINALITY);
	}

	protected double getPredicateCardinality(Var var, Collection<String> boundVars) {
		return getCardinality(var, boundVars, PREDICATE_VAR_CARDINALITY);
	}

	protected double getObjectCardinality(Var var, Collection<String> boundVars) {
		return getCardinality(var, boundVars, OBJECT_VAR_CARDINALITY);
	}

	protected double getContextCardinality(Var var, Collection<String> boundVars) {
		return getCardinality(var, boundVars, CONTEXT_VAR_CARDINALITY);
	}

	public static double getCardinality(Var var, Collection<String> boundVars, double varCardinality) {
		return hasValue(var, boundVars) ? 1.0 : varCardinality;
	}

	public static boolean hasValue(Var var, Collection<String> boundVars) {
		return var == null || var.hasValue() || boundVars.contains(var.getName());
	}

	@Override
	public void close() throws IOException {
	}
}
