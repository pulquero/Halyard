package com.msd.gin.halyard.sail.search;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;
import com.msd.gin.halyard.common.JavaObjectLiteral;
import com.msd.gin.halyard.model.vocabulary.HALYARD;
import com.msd.gin.halyard.query.algebra.Algebra;
import com.msd.gin.halyard.query.algebra.BGPCollector;
import com.msd.gin.halyard.query.algebra.ExtendedTupleFunctionCall;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
 * [] a halyard:Query;
 * halyard:query 'what';
 * halyard:limit 5;
 * halyard:matches [rdf:value ?v; halyard:score ?score; halyard:index ?index; halyard:field [rdfs:label "field_name"; rdf:value ?value] ]
 */
public class SearchInterpreter implements QueryOptimizer {

	@Override
	public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
		tupleExpr.visit(new SearchScanner());
	}

	static final class SearchScanner extends AbstractQueryModelVisitor<RuntimeException> {
		private void processGraphPattern(BGPCollector<RuntimeException> bgp) {
			ListMultimap<String, StatementPattern> stmtsBySubj = Multimaps.newListMultimap(new HashMap<>(), () -> new ArrayList<>(8));
			Map<Var, SearchCall> searchCallsBySubj = new HashMap<>();
			for (StatementPattern sp : bgp.getStatementPatterns()) {
				Var subjVar = sp.getSubjectVar();
				Var predVar = sp.getPredicateVar();
				Var objVar = sp.getObjectVar();
				if (RDF.TYPE.equals((IRI) predVar.getValue()) && HALYARD.QUERY_CLASS.equals(objVar.getValue())) {
					SearchCall searchCall = new SearchCall();
					searchCallsBySubj.put(subjVar, searchCall);
					sp.replaceWith(searchCall.tfc);
				} else {
					stmtsBySubj.put(subjVar.getName(), sp);
				}
			}

			for (Map.Entry<Var, SearchCall> entry : searchCallsBySubj.entrySet()) {
				String searchVarName = entry.getKey().getName();
				SearchCall searchCall = entry.getValue();
				List<StatementPattern> sps = stmtsBySubj.get(searchVarName);
				if (sps != null) {
					for (StatementPattern querySP : sps) {
						IRI queryPred = (IRI) querySP.getPredicateVar().getValue();
						Var queryObjVar = querySP.getObjectVar();
						if (HALYARD.QUERY_PROPERTY.equals(queryPred)) {
							querySP.replaceWith(new SingletonSet());
							searchCall.params.setQueryVar(queryObjVar);
						} else if (HALYARD.LIMIT_PROPERTY.equals(queryPred)) {
							querySP.replaceWith(new SingletonSet());
							searchCall.params.setLimitVar(queryObjVar);
						} else if (HALYARD.MIN_SCORE_PROPERTY.equals(queryPred)) {
							querySP.replaceWith(new SingletonSet());
							searchCall.params.setMinScoreVar(queryObjVar);
						} else if (HALYARD.FUZZINESS_PROPERTY.equals(queryPred)) {
							querySP.replaceWith(new SingletonSet());
							searchCall.params.setFuzzinessVar(queryObjVar);
						} else if (HALYARD.PHRASE_SLOP_PROPERTY.equals(queryPred)) {
							querySP.replaceWith(new SingletonSet());
							searchCall.params.setPhraseSlopVar(queryObjVar);
						} else if (HALYARD.MATCHES_PROPERTY.equals(queryPred)) {
							querySP.replaceWith(new SingletonSet());
							SearchParams.MatchParams matchParams = new SearchParams.MatchParams();
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
									SearchParams.MatchParams.FieldParams fieldParams = new SearchParams.MatchParams.FieldParams();
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
								searchCall.params.matches.add(matchParams);
							}
						}
					}
				}
			}

			for (SearchCall searchCall : searchCallsBySubj.values()) {
				if (!searchCall.initCall()) { // if invalid
					searchCall.tfc.replaceWith(new EmptySet());
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


	static final class SearchCall {
		static final ValueFactory VF = SimpleValueFactory.getInstance();
		final ExtendedTupleFunctionCall tfc = new ExtendedTupleFunctionCall(HALYARD.SEARCH.stringValue());
		final SearchParams params = new SearchParams();

		boolean initCall() {
			if (!params.isValid()) {
				return false;
			}
			tfc.addArg(params.queryVar != null ? params.queryVar.clone() : new ValueConstant(VF.createLiteral("")));
			tfc.addArg(params.limitVar != null ? params.limitVar.clone() : new ValueConstant(VF.createLiteral(SearchClient.DEFAULT_RESULT_SIZE)));
			tfc.addArg(params.minScoreVar != null ? params.minScoreVar.clone() : new ValueConstant(VF.createLiteral(SearchClient.DEFAULT_MIN_SCORE)));
			tfc.addArg(params.fuzzinessVar != null ? params.fuzzinessVar.clone() : new ValueConstant(VF.createLiteral(SearchClient.DEFAULT_FUZZINESS)));
			tfc.addArg(params.phraseSlopVar != null ? params.phraseSlopVar.clone() : new ValueConstant(VF.createLiteral(SearchClient.DEFAULT_PHRASE_SLOP)));
			tfc.addArg(new ValueConstant(JavaObjectLiteral.of(params.matches, Object.class)));
			for (SearchParams.MatchParams matchParams : params.matches) {
				for (String valueVar : matchParams.valueVars) {
					tfc.addResultVar(new Var(valueVar));
				}
				for (String scoreVar : matchParams.scoreVars) {
					tfc.addResultVar(new Var(scoreVar));
				}
				for (String indexVar : matchParams.indexVars) {
					tfc.addResultVar(new Var(indexVar));
				}
				for (SearchParams.MatchParams.FieldParams fieldParams : matchParams.fields) {
					for (String valueVar : fieldParams.valueVars) {
						tfc.addResultVar(new Var(valueVar));
					}
				}
			}
			return true;
		}
	}

	static final class SearchParams {
		Var queryVar;
		Var limitVar;
		Var minScoreVar;
		Var fuzzinessVar;
		Var phraseSlopVar;
		final List<MatchParams> matches = new ArrayList<>(1);
		boolean invalid;

		void setQueryVar(Var var) {
			if (queryVar == null) {
				queryVar = var;
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

		void setLimitVar(Var var) {
			if (limitVar == null) {
				limitVar = var;
			} else {
				invalid = true;
			}
		}

		void setFuzzinessVar(Var var) {
			if (fuzzinessVar == null) {
				fuzzinessVar = var;
			} else {
				invalid = true;
			}
		}

		void setPhraseSlopVar(Var var) {
			if (phraseSlopVar == null) {
				phraseSlopVar = var;
			} else {
				invalid = true;
			}
		}

		boolean isValid() {
			return !invalid && !matches.isEmpty();
		}

		static final class MatchParams implements Serializable {
			private static final long serialVersionUID = -1524678402469442919L;

			final List<String> valueVars = new ArrayList<>(1);
			final List<String> scoreVars = new ArrayList<>(1);
			final List<String> indexVars = new ArrayList<>(1);
			final List<FieldParams> fields = new ArrayList<>(1);

			boolean isValid() {
				return !valueVars.isEmpty() || !scoreVars.isEmpty() || !indexVars.isEmpty() || !fields.isEmpty();
			}

			@Override
			public boolean equals(Object o) {
				if (o == this) {
					return true;
				}
				if (o instanceof MatchParams) {
					MatchParams other = (MatchParams) o;
					return Objects.equals(valueVars, other.valueVars) && Objects.equals(scoreVars, other.scoreVars) && Objects.equals(indexVars, other.indexVars);
				} else {
					return false;
				}
			}

			@Override
			public int hashCode() {
				return Objects.hash(valueVars, scoreVars, indexVars);
			}


			static final class FieldParams implements Serializable {
				private static final long serialVersionUID = -326151903390393152L;

				String name;
				final List<String> valueVars = new ArrayList<>(1);

				boolean isValid() {
					return (name != null) && !valueVars.isEmpty();
				}

				@Override
				public boolean equals(Object o) {
					if (o == this) {
						return true;
					}
					if (o instanceof FieldParams) {
						FieldParams other = (FieldParams) o;
						return Objects.equals(name, other.name) && Objects.equals(valueVars, other.valueVars);
					} else {
						return false;
					}
				}

				@Override
				public int hashCode() {
					return Objects.hash(name, valueVars);
				}
			}
		}
	}
}
