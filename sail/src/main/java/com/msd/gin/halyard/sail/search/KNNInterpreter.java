package com.msd.gin.halyard.sail.search;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;
import com.msd.gin.halyard.common.JavaObjectLiteral;
import com.msd.gin.halyard.model.vocabulary.HALYARD;
import com.msd.gin.halyard.query.algebra.Algebra;
import com.msd.gin.halyard.query.algebra.BGPCollector;
import com.msd.gin.halyard.query.algebra.ExtendedTupleFunctionCall;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.Literals;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.EmptySet;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.SingletonSet;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.ValueConstant;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;

/**
 * [] a halyard:KNN;
 * halyard:query '[5.0, 2.0]';
 * halyard:k 5;
 * halyard:numCandidates 20;
 * halyard:minScore 0.4;
 * halyard:matches [rdf:value ?v; halyard:score ?score; halyard:index ?index; halyard:field [rdfs:label "field_name"; rdf:value ?value] ]
 */
public class KNNInterpreter implements QueryOptimizer {

	@Override
	public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
		tupleExpr.visit(new KNNScanner());
	}

	static final class KNNScanner extends AbstractQueryModelVisitor<RuntimeException> {
		private void processGraphPattern(BGPCollector<RuntimeException> bgp) {
			ListMultimap<String, StatementPattern> stmtsBySubj = Multimaps.newListMultimap(new HashMap<>(), () -> new ArrayList<>(8));
			Map<Var, KNNCall> knnCallsBySubj = new HashMap<>();
			for (StatementPattern sp : bgp.getStatementPatterns()) {
				Var subjVar = sp.getSubjectVar();
				Var predVar = sp.getPredicateVar();
				Var objVar = sp.getObjectVar();
				if (RDF.TYPE.equals((IRI) predVar.getValue()) && HALYARD.KNN_CLASS.equals(objVar.getValue())) {
					KNNCall knnCall = new KNNCall();
					knnCallsBySubj.put(subjVar, knnCall);
					sp.replaceWith(knnCall.tfc);
				} else {
					stmtsBySubj.put(subjVar.getName(), sp);
				}
			}

			for (Map.Entry<Var, KNNCall> entry : knnCallsBySubj.entrySet()) {
				String knnVarName = entry.getKey().getName();
				KNNCall knnCall = entry.getValue();
				List<StatementPattern> sps = stmtsBySubj.get(knnVarName);
				if (sps != null) {
					for (StatementPattern querySP : sps) {
						IRI queryPred = (IRI) querySP.getPredicateVar().getValue();
						Var queryObjVar = querySP.getObjectVar();
						if (HALYARD.QUERY_PROPERTY.equals(queryPred)) {
							querySP.replaceWith(new SingletonSet());
							knnCall.params.setQueryVar(queryObjVar);
						} else if (HALYARD.K_PROPERTY.equals(queryPred)) {
							querySP.replaceWith(new SingletonSet());
							knnCall.params.setKVar(queryObjVar);
						} else if (HALYARD.NUM_CANDIDATES_PROPERTY.equals(queryPred)) {
							querySP.replaceWith(new SingletonSet());
							knnCall.params.setNumCandidatesVar(queryObjVar);
						} else if (HALYARD.MIN_SCORE_PROPERTY.equals(queryPred)) {
							querySP.replaceWith(new SingletonSet());
							knnCall.params.setMinScoreVar(queryObjVar);
						} else if (HALYARD.MATCHES_PROPERTY.equals(queryPred)) {
							querySP.replaceWith(new SingletonSet());
							MatchParams matchParams = new MatchParams();
							for (StatementPattern matchSP : stmtsBySubj.get(queryObjVar.getName())) {
								IRI matchPred = (IRI) matchSP.getPredicateVar().getValue();
								Var matchObjVar = matchSP.getObjectVar();
								if (RDF.VALUE.equals(matchPred)) {
									matchSP.replaceWith(new SingletonSet());
									if (Algebra.isFree(matchObjVar)) {
										matchParams.valueVars.add(matchObjVar.getName());
									}
								} else if (HALYARD.SCORE_PROPERTY.equals(matchPred)) {
									matchSP.replaceWith(new SingletonSet());
									if (Algebra.isFree(matchObjVar)) {
										matchParams.scoreVars.add(matchObjVar.getName());
									}
								} else if (HALYARD.INDEX_PROPERTY.equals(matchPred)) {
									matchSP.replaceWith(new SingletonSet());
									if (Algebra.isFree(matchObjVar)) {
										matchParams.indexVars.add(matchObjVar.getName());
									}
								} else if (HALYARD.FIELD_PROPERTY.equals(matchPred)) {
									matchSP.replaceWith(new SingletonSet());
									MatchParams.FieldParams fieldParams = new MatchParams.FieldParams();
									for (StatementPattern fieldSP : stmtsBySubj.get(matchObjVar.getName())) {
										IRI fieldPred = (IRI) fieldSP.getPredicateVar().getValue();
										Var fieldObjVar = fieldSP.getObjectVar();
										if (RDFS.LABEL.equals(fieldPred)) {
											fieldSP.replaceWith(new SingletonSet());
											Value labelValue = fieldObjVar.getValue();
											fieldParams.name = Literals.getLabel(labelValue, null);
										} else if (RDF.VALUE.equals(fieldPred)) {
											fieldSP.replaceWith(new SingletonSet());
											if (Algebra.isFree(fieldObjVar)) {
												fieldParams.valueVars.add(fieldObjVar.getName());
											}
										}
									}
									if (!Algebra.isFree(matchObjVar) && fieldParams.isValid()) {
										matchParams.fields.add(fieldParams);
									}
								}
							}
							if (!Algebra.isFree(queryObjVar) && matchParams.isValid()) {
								knnCall.params.matches.add(matchParams);
							}
						}
					}
				}
			}

			for (KNNCall knnCall : knnCallsBySubj.values()) {
				if (!knnCall.initCall()) { // if invalid
					knnCall.tfc.replaceWith(new EmptySet());
				}
			}
		}

		@Override
		public void meet(Join join) {
			BGPCollector<RuntimeException> collector = new BGPCollector<>(this);
			join.visit(collector);
			processGraphPattern(collector);
		}

		@Override
		public void meet(Service node) {
			// leave for the remote endpoint to interpret
		}
	}


	static final class KNNCall {
		static final ValueFactory VF = SimpleValueFactory.getInstance();
		final ExtendedTupleFunctionCall tfc = new ExtendedTupleFunctionCall(HALYARD.KNN_FUNCTION.stringValue());
		final KNNParams params = new KNNParams();

		boolean initCall() {
			if (!params.isValid()) {
				return false;
			}
			tfc.addArg(params.queryVar != null ? params.queryVar.clone() : new ValueConstant(VF.createLiteral("")));
			tfc.addArg(params.kVar != null ? params.kVar.clone() : new ValueConstant(VF.createLiteral(SearchClient.DEFAULT_K)));
			tfc.addArg(params.numCandidatesVar != null ? params.numCandidatesVar.clone() : new ValueConstant(VF.createLiteral(SearchClient.DEFAULT_NUM_CANDIDATES)));
			tfc.addArg(params.minScoreVar != null ? params.minScoreVar.clone() : new ValueConstant(VF.createLiteral(SearchClient.DEFAULT_MIN_SCORE)));
			tfc.addArg(new ValueConstant(JavaObjectLiteral.of(params.matches, Object.class)));
			for (MatchParams matchParams : params.matches) {
				for (String valueVar : matchParams.valueVars) {
					tfc.addResultVar(new Var(valueVar));
				}
				for (String scoreVar : matchParams.scoreVars) {
					tfc.addResultVar(new Var(scoreVar));
				}
				for (String indexVar : matchParams.indexVars) {
					tfc.addResultVar(new Var(indexVar));
				}
				for (MatchParams.FieldParams fieldParams : matchParams.fields) {
					for (String valueVar : fieldParams.valueVars) {
						tfc.addResultVar(new Var(valueVar));
					}
				}
			}
			return true;
		}
	}

	static final class KNNParams {
		Var queryVar;
		Var kVar;
		Var numCandidatesVar;
		Var minScoreVar;
		final List<MatchParams> matches = new ArrayList<>(1);
		boolean invalid;

		void setQueryVar(Var var) {
			if (queryVar == null) {
				queryVar = var;
			} else {
				invalid = true;
			}
		}

		void setKVar(Var var) {
			if (kVar == null) {
				kVar = var;
			} else {
				invalid = true;
			}
		}

		void setNumCandidatesVar(Var var) {
			if (numCandidatesVar == null) {
				numCandidatesVar = var;
			} else {
				invalid = true;
			}
		}

		void setMinScoreVar(Var var) {
			if (minScoreVar == null) {
				minScoreVar = var;
			} else {
				invalid = true;
			}
		}

		boolean isValid() {
			return !invalid && !matches.isEmpty();
		}
	}
}
