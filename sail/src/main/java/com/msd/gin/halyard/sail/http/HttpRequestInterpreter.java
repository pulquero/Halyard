package com.msd.gin.halyard.sail.http;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;
import com.msd.gin.halyard.model.ObjectArrayLiteral;
import com.msd.gin.halyard.model.vocabulary.HALYARD;
import com.msd.gin.halyard.query.algebra.BGPCollector;
import com.msd.gin.halyard.query.algebra.ExtendedTupleFunctionCall;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
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
 * @see https://www.w3.org/TR/HTTP-in-RDF10/
 */
public class HttpRequestInterpreter implements QueryOptimizer {

	@Override
	public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
		tupleExpr.visit(new HttpScanner());
	}

	static final class HttpScanner extends AbstractQueryModelVisitor<RuntimeException> {
		private void processGraphPattern(BGPCollector<RuntimeException> bgp) {
			ListMultimap<String, StatementPattern> stmtsBySubj = Multimaps.newListMultimap(new HashMap<>(), () -> new ArrayList<>(8));
			Map<Var, HttpCall> httpCallsBySubj = new HashMap<>();
			for (StatementPattern sp : bgp.getStatementPatterns()) {
				Var subjVar = sp.getSubjectVar();
				Var predVar = sp.getPredicateVar();
				Var objVar = sp.getObjectVar();
				if (RDF.TYPE.equals((IRI) predVar.getValue()) && HTTP.REQUEST_CLASS.equals(objVar.getValue())) {
					HttpCall httpCall = new HttpCall();
					httpCallsBySubj.put(subjVar, httpCall);
					sp.replaceWith(httpCall.tfc);
				} else {
					stmtsBySubj.put(subjVar.getName(), sp);
				}
			}

			for (Map.Entry<Var, HttpCall> entry : httpCallsBySubj.entrySet()) {
				String httpVarName = entry.getKey().getName();
				HttpCall httpCall = entry.getValue();
				List<StatementPattern> sps = stmtsBySubj.get(httpVarName);
				if (sps != null) {
					for (StatementPattern httpSP : sps) {
						IRI httpPred = (IRI) httpSP.getPredicateVar().getValue();
						Var httpObjVar = httpSP.getObjectVar();
						if (HTTP.ABSOLUTE_URI_PROPERTY.equals(httpPred)) {
							httpSP.replaceWith(new SingletonSet());
							httpCall.params.setAbsoluteURI(httpObjVar);
						} else if (HTTP.METHOD_NAME_PROPERTY.equals(httpPred)) {
							httpSP.replaceWith(new SingletonSet());
							httpCall.params.setMethod(httpObjVar);
						} else if (HTTP.BODY_PROPERTY.equals(httpPred)) {
							httpSP.replaceWith(new SingletonSet());
							httpCall.params.setRequestBody(httpObjVar);
						} else if (HTTP.RESP_PROPERTY.equals(httpPred)) {
							httpSP.replaceWith(new SingletonSet());
							List<StatementPattern> respSPs = stmtsBySubj.get(httpObjVar.getName());
							for (StatementPattern respSP : respSPs) {
								IRI respPred = (IRI) respSP.getPredicateVar().getValue();
								Var respObj = respSP.getObjectVar();
								if (HTTP.BODY_PROPERTY.equals(respPred)) {
									respSP.replaceWith(new SingletonSet());
									httpCall.params.setResponseBodyVarName(respObj.getName());
								} else if (HTTP.STATUS_CODE_VALUE_PROPERTY.equals(respPred)) {
									respSP.replaceWith(new SingletonSet());
									httpCall.params.setStatusCodeVarName(respObj.getName());
								} else if (HTTP.REASON_PHRASE_PROPERTY.equals(respPred)) {
									respSP.replaceWith(new SingletonSet());
									httpCall.params.setReasonVarName(respObj.getName());
								}
							}
						} else if (HTTP.HEADERS_PROPERTY.equals(httpPred)) {
							httpSP.replaceWith(new SingletonSet());
							visitList(httpObjVar, stmtsBySubj, var -> {
								String fieldName = null;
								String fieldValue = null;
								List<StatementPattern> headerSPs = stmtsBySubj.get(var.getName());
								for (StatementPattern headerSP : headerSPs) {
									IRI headerPred = (IRI) headerSP.getPredicateVar().getValue();
									Var headerObjVar = headerSP.getObjectVar();
									if (HTTP.FIELD_NAME_PROPERTY.equals(headerPred)) {
										headerSP.replaceWith(new SingletonSet());
										fieldName = headerObjVar.getValue().stringValue();
									} else if (HTTP.FIELD_VALUE_PROPERTY.equals(headerPred)) {
										headerSP.replaceWith(new SingletonSet());
										fieldValue = headerObjVar.getValue().stringValue();
									}
								}
								if (fieldName != null && fieldValue != null) {
									httpCall.params.addHeader(fieldName, fieldValue);
								}
							});
						}
					}
				}
			}

			for (HttpCall httpCall : httpCallsBySubj.values()) {
				if (!httpCall.initCall()) {
					httpCall.tfc.replaceWith(new EmptySet());
				}
			}
		}

		private void visitList(Var listVar, ListMultimap<String, StatementPattern> stmtsBySubj, Consumer<Var> visitor) {
			Var restVar = null;
			List<StatementPattern> listElement = stmtsBySubj.get(listVar.getName());
			for (StatementPattern sp : listElement) {
				IRI pred = (IRI) sp.getPredicateVar().getValue();
				Var objVar = sp.getObjectVar();
				if (RDF.FIRST.equals(pred)) {
					sp.replaceWith(new SingletonSet());
					visitor.accept(objVar);
				} else if (RDF.REST.equals(pred)) {
					sp.replaceWith(new SingletonSet());
					restVar = objVar;
				}
			}
			if (restVar != null && !RDF.NIL.equals(restVar.getValue())) {
				visitList(restVar, stmtsBySubj, visitor);
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

	static final class HttpCall {
		static final ValueFactory VF = SimpleValueFactory.getInstance();
		final ExtendedTupleFunctionCall tfc = new ExtendedTupleFunctionCall(HALYARD.HTTP_REQUEST_FUNCTION.stringValue());
		final HttpParams params = new HttpParams();

		boolean initCall() {
			if (params.statusCodeVarName == null || params.reasonVarName == null || params.responseBodyVarName == null) {
				return false;
			}
			tfc.addArg(params.absoluteURIVar.clone());
			tfc.addArg(params.methodVar != null ? params.methodVar.clone() : new ValueConstant(VF.createLiteral("GET")));
			tfc.addArg(new ValueConstant(new ObjectArrayLiteral(params.headers.toArray(), Pair.class)));
			tfc.addArg(params.requestBodyVar != null ? params.requestBodyVar.clone() : new ValueConstant(RDF.NIL));
			tfc.addResultVar(new Var(params.statusCodeVarName));
			tfc.addResultVar(new Var(params.reasonVarName));
			tfc.addResultVar(new Var(params.responseBodyVarName));
			return true;
		}
	}

	static final class HttpParams {
		Var absoluteURIVar;
		Var methodVar;
		Var requestBodyVar;
		List<Pair<String, String>> headers = new ArrayList<>();
		String responseBodyVarName;
		String statusCodeVarName;
		String reasonVarName;

		void setAbsoluteURI(Var var) {
			absoluteURIVar = var;
		}

		void setMethod(Var var) {
			methodVar = var;
		}

		void setRequestBody(Var var) {
			requestBodyVar = var;
		}

		void addHeader(String name, String value) {
			headers.add(Pair.of(name, value));
		}

		void setResponseBodyVarName(String var) {
			responseBodyVarName = var;
		}

		void setStatusCodeVarName(String var) {
			statusCodeVarName = var;
		}

		void setReasonVarName(String var) {
			reasonVarName = var;
		}
	}
}
