/*******************************************************************************
 * Copyright (c) 2015 Eclipse RDF4J contributors, Aduna, and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 *******************************************************************************/
package com.msd.gin.halyard.spin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.eclipse.rdf4j.common.exception.RDF4JException;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.model.vocabulary.SP;
import org.eclipse.rdf4j.model.vocabulary.SPIN;
import org.eclipse.rdf4j.model.vocabulary.SPL;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Union;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.AbstractFederatedServiceResolver;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolver;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunction;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunctionRegistry;
import org.eclipse.rdf4j.query.algebra.evaluation.util.TripleSources;
import org.eclipse.rdf4j.query.algebra.helpers.TupleExprs;
import org.eclipse.rdf4j.query.parser.ParsedTupleQuery;
import org.eclipse.rdf4j.queryrender.sparql.SPARQLQueryRenderer;
import org.eclipse.rdf4j.sail.Sail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.msd.gin.halyard.query.algebra.AbstractExtendedQueryModelVisitor;
import com.msd.gin.halyard.query.algebra.Algebra;
import com.msd.gin.halyard.query.algebra.BGPCollector;
import com.msd.gin.halyard.query.algebra.ExtendedTupleFunctionCall;
import com.msd.gin.halyard.query.algebra.StarJoin;
import com.msd.gin.halyard.query.algebra.evaluation.CloseableTripleSource;
import com.msd.gin.halyard.query.algebra.evaluation.federation.SailFederatedService;
import com.msd.gin.halyard.query.algebra.evaluation.federation.TupleFunctionFederatedService;
import com.msd.gin.halyard.spin.function.ConstructTupleFunction;
import com.msd.gin.halyard.spin.function.InverseMagicProperty;
import com.msd.gin.halyard.spin.function.SelectTupleFunction;

public class SpinMagicPropertyInterpreter implements QueryOptimizer {

	private static final Logger logger = LoggerFactory.getLogger(SpinMagicPropertyInterpreter.class);

	private static final String SPIN_SERVICE = "spin:/";

	public static void registerSpinParsingTupleFunctions(SpinParser parser, TupleFunctionRegistry tupleFunctionRegistry) {
		if (!tupleFunctionRegistry.has(SPIN.CONSTRUCT_PROPERTY.stringValue())) {
			tupleFunctionRegistry.add(new ConstructTupleFunction(parser));
		}
		if (!tupleFunctionRegistry.has(SPIN.SELECT_PROPERTY.stringValue())) {
			tupleFunctionRegistry.add(new SelectTupleFunction(parser));
		}
	}

	private final SpinParser parser;
	private final TripleSource tripleSource;
	private final TupleFunctionRegistry tupleFunctionRegistry;
	private final FederatedServiceResolver serviceResolver;
	private final Supplier<CloseableTripleSource> tripleSourceFactory;
	private final boolean includeMatchingTriples;

	/**
	 * Constructs a SpinMagicPropertyInterpreter using native evaluation.
	 * @param parser
	 * @param tripleSource
	 * @param tupleFunctionRegistry
	 * @param serviceResolver
	 */
	public SpinMagicPropertyInterpreter(SpinParser parser, TripleSource tripleSource,
			TupleFunctionRegistry tupleFunctionRegistry, FederatedServiceResolver serviceResolver) {
		this(parser, tripleSource, tupleFunctionRegistry, serviceResolver, true);
	}

	/**
	 * Constructs a SpinMagicPropertyInterpreter using native evaluation.
	 * @param parser
	 * @param tripleSource
	 * @param tupleFunctionRegistry
	 * @param serviceResolver
	 * @param includeTriples
	 */
	public SpinMagicPropertyInterpreter(SpinParser parser, TripleSource tripleSource,
			TupleFunctionRegistry tupleFunctionRegistry, FederatedServiceResolver serviceResolver, boolean includeTriples) {
		this(parser, tripleSource, tupleFunctionRegistry, serviceResolver, null, includeTriples);
	}

	/**
	 * Constructs a SpinMagicPropertyInterpreter.
	 * @param parser
	 * @param tripleSource
	 * @param tupleFunctionRegistry
	 * @param serviceResolver
	 * @param tripleSourceFactory non-null to use the SPIN federated service for evaluation.
	 * @param includeTriples
	 */
	public SpinMagicPropertyInterpreter(SpinParser parser, TripleSource tripleSource, TupleFunctionRegistry tupleFunctionRegistry, FederatedServiceResolver serviceResolver, Supplier<CloseableTripleSource> tripleSourceFactory, boolean includeTriples) {
		this.parser = parser;
		this.tripleSource = tripleSource;
		this.tupleFunctionRegistry = tupleFunctionRegistry;
		this.serviceResolver = serviceResolver;
		this.tripleSourceFactory = tripleSourceFactory;
		this.includeMatchingTriples = includeTriples;
	}

	@Override
	public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
		try {
			tupleExpr.visit(new MagicPropertyScanner(parser, tripleSource, tupleFunctionRegistry, serviceResolver, tripleSourceFactory, includeMatchingTriples));
		} catch (RDF4JException e) {
			logger.warn("Failed to parse tuple function");
		}
	}


	private static class MagicPropertyScanner extends AbstractExtendedQueryModelVisitor<RDF4JException> {
		private final SpinParser parser;
		private final TripleSource tripleSource;
		private final TupleFunctionRegistry tupleFunctionRegistry;
		private final FederatedServiceResolver serviceResolver;
		private final Supplier<CloseableTripleSource> tripleSourceFactory;
		private final boolean includeMatchingTriples;
		private final IRI spinServiceUri;

		MagicPropertyScanner(SpinParser parser, TripleSource tripleSource, TupleFunctionRegistry tupleFunctionRegistry, FederatedServiceResolver serviceResolver, Supplier<CloseableTripleSource> tripleSourceFactory, boolean includeTriples) {
			this.parser = parser;
			this.tripleSource = tripleSource;
			this.tupleFunctionRegistry = tupleFunctionRegistry;
			this.serviceResolver = serviceResolver;
			this.tripleSourceFactory = tripleSourceFactory;
			this.includeMatchingTriples = includeTriples;
			this.spinServiceUri = tripleSource.getValueFactory().createIRI(SPIN_SERVICE);
		}

		private void processGraphPattern(List<StatementPattern> sps) {
			Map<StatementPattern, TupleFunction> magicProperties = new LinkedHashMap<>();
			Map<String, Map<IRI, List<StatementPattern>>> spIndex = new HashMap<>();

			for (StatementPattern sp : sps) {
				IRI pred = (IRI) sp.getPredicateVar().getValue();
				if (pred != null) {
					TupleFunction func = tupleFunctionRegistry.get(pred.stringValue()).orElse(null);
					if (func != null) {
						magicProperties.put(sp, func);
					} else {
						Statement magicPropStmt = TripleSources.single(pred, RDF.TYPE, SPIN.MAGIC_PROPERTY_CLASS,
								tripleSource);
						if (magicPropStmt != null) {
							func = parser.parseMagicProperty(pred, tripleSource);
							tupleFunctionRegistry.add(func);
							magicProperties.put(sp, func);
						} else {
							// normal statement
							String subj = sp.getSubjectVar().getName();
							Map<IRI, List<StatementPattern>> predMap = spIndex.computeIfAbsent(subj, k -> new HashMap<>(8));
							List<StatementPattern> v = predMap.computeIfAbsent(pred, k -> new ArrayList<>(1));
							v.add(sp);
						}
					}
				}
			}

			if (!magicProperties.isEmpty()) {
				for (Map.Entry<StatementPattern, TupleFunction> entry : magicProperties.entrySet()) {
					final StatementPattern sp = entry.getKey();
					final TupleFunction func = entry.getValue();
					TupleExpr stmts = sp.clone();

					List<? super Var> clonedSubjList = new ArrayList<ValueExpr>(4);
					TupleExpr subjNodes = addVarsFromRdfList(clonedSubjList, sp.getSubjectVar(), spIndex);
					if (subjNodes != null) {
						stmts = new Join(stmts, subjNodes);
					} else {
						clonedSubjList = Collections.<ValueExpr>singletonList(sp.getSubjectVar().clone());
					}

					List<? super Var> clonedObjList = new ArrayList<ValueExpr>(4);
					TupleExpr objNodes = addVarsFromRdfList(clonedObjList, sp.getObjectVar(), spIndex);
					if (objNodes != null) {
						stmts = new Join(stmts, objNodes);
					} else {
						clonedObjList = Collections.<ValueExpr>singletonList(sp.getObjectVar().clone());
					}

					ExtendedTupleFunctionCall funcCall = new ExtendedTupleFunctionCall(func.getURI());
					if (func instanceof InverseMagicProperty) {
						funcCall.setArgs((List<ValueExpr>) clonedObjList);
						funcCall.setResultVars((List<Var>) clonedSubjList);
					} else {
						funcCall.setArgs((List<ValueExpr>) clonedSubjList);
						funcCall.setResultVars((List<Var>) clonedObjList);
					}

					TupleExpr magicPropertyNode;
					if (tripleSourceFactory == null) {
						// native evaluation
						magicPropertyNode = funcCall;
					} else {
						// use SERVICE evaluation
						AbstractFederatedServiceResolver fedResolver = (AbstractFederatedServiceResolver) serviceResolver;
						if (!fedResolver.hasService(SPIN_SERVICE)) {
							fedResolver.registerService(SPIN_SERVICE, new TupleFunctionFederatedService(tripleSourceFactory, tupleFunctionRegistry));
						}

						Var serviceRef = TupleExprs.createConstVar(spinServiceUri);
						String exprString;
						try {
							exprString = new SPARQLQueryRenderer().render(new ParsedTupleQuery(stmts));
							exprString = exprString.substring(exprString.indexOf('{') + 1, exprString.lastIndexOf('}'));
						} catch (Exception e) {
							throw new MalformedQueryException(e);
						}
						Map<String, String> prefixDecls = new HashMap<>(8);
						prefixDecls.put(SP.PREFIX, SP.NAMESPACE);
						prefixDecls.put(SPIN.PREFIX, SPIN.NAMESPACE);
						prefixDecls.put(SPL.PREFIX, SPL.NAMESPACE);
						magicPropertyNode = new Service(serviceRef, funcCall, exprString, prefixDecls, null, false);
					}

					if (includeMatchingTriples && hasTriplesWithMagicProperty(func.getURI())) {
						Union union = new Union();
						sp.replaceWith(union);
						union.setLeftArg(stmts);
						union.setRightArg(magicPropertyNode);
					} else {
						// don't need to worry about union-ing existing triples with the magic property
						sp.replaceWith(magicPropertyNode);
					}
				}
			}
		}

		private boolean hasTriplesWithMagicProperty(String property) {
			try (CloseableIteration<? extends Statement, QueryEvaluationException> iter = tripleSource.getStatements(null, tripleSource.getValueFactory().createIRI(property), null)) {
				return iter.hasNext();
			}
		}

		private TupleExpr join(TupleExpr node, TupleExpr toMove) {
			Algebra.remove(toMove);
			if (node != null) {
				node = new Join(node, toMove);
			} else {
				node = toMove;
			}
			return node;
		}

		private TupleExpr addVarsFromRdfList(List<? super Var> clonedVars, Var subj,
				Map<String, Map<IRI, List<StatementPattern>>> spIndex) {
			TupleExpr node = null;
			do {
				Map<IRI, List<StatementPattern>> predMap = spIndex.get(subj.getName());
				if (predMap == null) {
					return null;
				}

				List<StatementPattern> firstStmts = predMap.get(RDF.FIRST);
				if (firstStmts == null) {
					return null;
				}
				if (firstStmts.size() != 1) {
					return null;
				}

				List<StatementPattern> restStmts = predMap.get(RDF.REST);
				if (restStmts == null) {
					return null;
				}
				if (restStmts.size() != 1) {
					return null;
				}

				StatementPattern firstStmt = firstStmts.get(0);
				clonedVars.add(firstStmt.getObjectVar().clone());
				node = join(node, firstStmt);

				StatementPattern restStmt = restStmts.get(0);
				subj = restStmt.getObjectVar();
				node = join(node, restStmt);

				List<StatementPattern> typeStmts = predMap.get(RDF.TYPE);
				if (typeStmts != null) {
					for (StatementPattern sp : firstStmts) {
						Value type = sp.getObjectVar().getValue();
						if (RDFS.RESOURCE.equals(type) || RDF.LIST.equals(type)) {
							node = join(node, sp);
						}
					}
				}
			} while (!RDF.NIL.equals(subj.getValue()));
			return node;
		}

		@Override
		public void meet(Join node) {
			BGPCollector<RDF4JException> collector = new BGPCollector<>(this);
			node.visit(collector);
			processGraphPattern(collector.getStatementPatterns());
		}

		@Override
		public void meet(StatementPattern node) {
			processGraphPattern(Collections.singletonList(node));
		}

		@Override
		public void meet(StarJoin node) {
			BGPCollector<RDF4JException> collector = new BGPCollector<>(this);
			for (TupleExpr arg : node.getArgs()) {
				arg.visit(collector);
			}
			processGraphPattern(collector.getStatementPatterns());
		}

		@Override
		public void meet(Service node) {
			IRI serviceUrl = (IRI) node.getServiceRef().getValue();
			if (serviceUrl != null) {
				FederatedService fs = serviceResolver.getService(serviceUrl.stringValue());
				if (fs instanceof SailFederatedService) {
					SailFederatedService sfs = (SailFederatedService) fs;
					Sail sail = sfs.getSail();
					if (sail instanceof SpinSail) {
						SpinSail spinSail = (SpinSail) sail;
						TupleFunctionRegistry serviceTFRegistry = spinSail.getTupleFunctionRegistry();
						try (CloseableTripleSource serviceTripleSource = spinSail.newTripleSource()) {
							MagicPropertyScanner serviceScanner = new MagicPropertyScanner(spinSail.getSpinParser(), serviceTripleSource, serviceTFRegistry, spinSail.getFederatedServiceResolver(), null, includeMatchingTriples);
							node.getServiceExpr().visit(serviceScanner);
						}
					}
				}
			}
			// else leave for the remote endpoint to interpret
		}
	}
}
