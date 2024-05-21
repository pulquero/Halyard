/*******************************************************************************
 * Copyright (c) 2015 Eclipse RDF4J contributors, Aduna, and others.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 *******************************************************************************/
package com.msd.gin.halyard.queryparser;

import com.msd.gin.halyard.query.algebra.ExtendedQueryRoot;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.util.Namespaces;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.IncompatibleOperationException;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.UnsupportedQueryLanguageException;
import org.eclipse.rdf4j.query.algebra.DeleteData;
import org.eclipse.rdf4j.query.algebra.InsertData;
import org.eclipse.rdf4j.query.algebra.QueryRoot;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.UpdateExpr;
import org.eclipse.rdf4j.query.parser.ParsedBooleanQuery;
import org.eclipse.rdf4j.query.parser.ParsedDescribeQuery;
import org.eclipse.rdf4j.query.parser.ParsedGraphQuery;
import org.eclipse.rdf4j.query.parser.ParsedOperation;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.ParsedTupleQuery;
import org.eclipse.rdf4j.query.parser.ParsedUpdate;
import org.eclipse.rdf4j.query.parser.QueryParser;
import org.eclipse.rdf4j.query.parser.sparql.BaseDeclProcessor;
import org.eclipse.rdf4j.query.parser.sparql.BlankNodeVarProcessor;
import org.eclipse.rdf4j.query.parser.sparql.DatasetDeclProcessor;
import org.eclipse.rdf4j.query.parser.sparql.PrefixDeclProcessor;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLUpdateDataBlockParser;
import org.eclipse.rdf4j.query.parser.sparql.StringEscapesProcessor;
import org.eclipse.rdf4j.query.parser.sparql.WildcardProjectionProcessor;
import org.eclipse.rdf4j.query.parser.sparql.ast.ASTAskQuery;
import org.eclipse.rdf4j.query.parser.sparql.ast.ASTConstructQuery;
import org.eclipse.rdf4j.query.parser.sparql.ast.ASTDescribeQuery;
import org.eclipse.rdf4j.query.parser.sparql.ast.ASTInsertData;
import org.eclipse.rdf4j.query.parser.sparql.ast.ASTPrefixDecl;
import org.eclipse.rdf4j.query.parser.sparql.ast.ASTQuery;
import org.eclipse.rdf4j.query.parser.sparql.ast.ASTQueryContainer;
import org.eclipse.rdf4j.query.parser.sparql.ast.ASTSelectQuery;
import org.eclipse.rdf4j.query.parser.sparql.ast.ASTUpdate;
import org.eclipse.rdf4j.query.parser.sparql.ast.ASTUpdateContainer;
import org.eclipse.rdf4j.query.parser.sparql.ast.ASTUpdateSequence;
import org.eclipse.rdf4j.query.parser.sparql.ast.Node;
import org.eclipse.rdf4j.query.parser.sparql.ast.ParseException;
import org.eclipse.rdf4j.query.parser.sparql.ast.SyntaxTreeBuilder;
import org.eclipse.rdf4j.query.parser.sparql.ast.TokenMgrError;
import org.eclipse.rdf4j.query.parser.sparql.ast.VisitorException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;

public class SPARQLParser implements QueryParser {
	public static ParsedUpdate parseUpdate(String update, String baseURI, ValueFactory vf)
			throws MalformedQueryException, UnsupportedQueryLanguageException {
		QueryParser parser = new SPARQLParser(vf);
		return parser.parseUpdate(update, baseURI);
	}

	public static ParsedQuery parseQuery(String query, String baseURI, ValueFactory vf)
			throws MalformedQueryException, UnsupportedQueryLanguageException {
		QueryParser parser = new SPARQLParser(vf);
		return parser.parseQuery(query, baseURI);
	}

	public static ParsedTupleQuery parseTupleQuery(String query, String baseURI, ValueFactory vf)
			throws MalformedQueryException, UnsupportedQueryLanguageException {
		ParsedOperation q = parseQuery(query, baseURI, vf);

		if (q instanceof ParsedTupleQuery) {
			return (ParsedTupleQuery) q;
		}

		throw new IllegalArgumentException("query is not a tuple query: " + query);
	}

	public static ParsedGraphQuery parseGraphQuery(String query, String baseURI, ValueFactory vf)
			throws MalformedQueryException, UnsupportedQueryLanguageException {
		ParsedOperation q = parseQuery(query, baseURI, vf);

		if (q instanceof ParsedGraphQuery) {
			return (ParsedGraphQuery) q;
		}

		throw new IllegalArgumentException("query is not a graph query: " + query);
	}

	public static ParsedBooleanQuery parseBooleanQuery(String query, String baseURI, ValueFactory vf)
			throws MalformedQueryException, UnsupportedQueryLanguageException {
		ParsedOperation q = parseQuery(query, baseURI, vf);

		if (q instanceof ParsedBooleanQuery) {
			return (ParsedBooleanQuery) q;
		}

		throw new IllegalArgumentException("query is not a boolean query: " + query);
	}


	private final ValueFactory valueFactory;
	private final Map<String, String> customPrefixes;

	/**
	 * Create a new SPARQLParser.
	 *
	 * @param valueFactory
	 * @param customPrefixes the default namespaces to apply to this parser. null for no prefixes
	 */
	public SPARQLParser(ValueFactory valueFactory, Set<Namespace> customPrefixes) {
		this.valueFactory = new ParsingValueFactory(Objects.requireNonNull(valueFactory));
		Objects.requireNonNull(customPrefixes, "customPrefixes can't be null!");
		if (customPrefixes.isEmpty()) {
			this.customPrefixes = Collections.emptyMap();
		} else {
			this.customPrefixes = Namespaces.asMap(customPrefixes);
		}
	}

	/**
	 * Create a new SPARQLParser without any default prefixes.
	 */
	public SPARQLParser(ValueFactory valueFactory) {
		this(valueFactory, Collections.emptySet());
	}

	@Override
	public ParsedUpdate parseUpdate(String updateStr, String baseURI) throws MalformedQueryException {
		try {

			ParsedUpdate update = new ParsedUpdate(updateStr);

			SPARQLUpdateDataBlockParser parser = new SPARQLUpdateDataBlockParser();

			ASTUpdateSequence updateSequence = SyntaxTreeBuilder.parseUpdateSequence(updateStr);

			List<ASTUpdateContainer> updateOperations = updateSequence.getUpdateContainers();

			List<ASTPrefixDecl> sharedPrefixDeclarations = null;

			Node node = updateSequence.jjtGetChild(0);

			Set<String> globalUsedBNodeIds = new HashSet<>();
			for (int i = 0; i < updateOperations.size(); i++) {

				ASTUpdateContainer uc = updateOperations.get(i);

				if (uc.jjtGetNumChildren() == 0 && i > 0 && i < updateOperations.size() - 1) {
					// empty update in the middle of the sequence
					throw new MalformedQueryException("empty update in sequence not allowed");
				}

				StringEscapesProcessor.process(uc);
				BaseDeclProcessor.process(uc, baseURI);
				WildcardProjectionProcessor.process(uc);

				if (uc.getBaseDecl() != null) {
					baseURI = uc.getBaseDecl().getIRI();
				}

				// do a special dance to handle prefix declarations in sequences: if
				// the current
				// operation has its own prefix declarations, use those. Otherwise,
				// try and use
				// prefix declarations from a previous operation in this sequence.
				List<ASTPrefixDecl> prefixDeclList = uc.getPrefixDeclList();
				if (prefixDeclList == null || prefixDeclList.isEmpty()) {
					if (sharedPrefixDeclarations != null) {
						for (ASTPrefixDecl prefixDecl : sharedPrefixDeclarations) {
							uc.jjtAppendChild(prefixDecl);
						}
					}
				} else {
					sharedPrefixDeclarations = prefixDeclList;
				}

				PrefixDeclProcessor.process(uc, customPrefixes);
				Set<String> usedBNodeIds = BlankNodeVarProcessor.process(uc);

				if (uc.getUpdate() instanceof ASTInsertData || uc.getUpdate() instanceof ASTInsertData) {
					if (Collections.disjoint(usedBNodeIds, globalUsedBNodeIds)) {
						globalUsedBNodeIds.addAll(usedBNodeIds);
					} else {
						throw new MalformedQueryException("blank node identifier may not be shared across INSERT/DELETE DATA operations");
					}
				}

				UpdateExprBuilder updateExprBuilder = new UpdateExprBuilder(valueFactory);

				ASTUpdate updateNode = uc.getUpdate();
				if (updateNode != null) {
					UpdateExpr updateExpr = (UpdateExpr) updateNode.jjtAccept(updateExprBuilder, null);

					// add individual update expression to ParsedUpdate sequence
					// container

					String datablock = "";
					if (updateExpr instanceof InsertData) {
						InsertData insertDataExpr = (InsertData) updateExpr;
						parser.getParserConfig().set(BasicParserSettings.SKOLEMIZE_ORIGIN, null);
						parser.setLineNumberOffset(insertDataExpr.getLineNumberOffset());
						datablock = insertDataExpr.getDataBlock();
					} else if (updateExpr instanceof DeleteData) {
						DeleteData deleteDataExpr = (DeleteData) updateExpr;
						parser.setLineNumberOffset(deleteDataExpr.getLineNumberOffset());
						parser.setAllowBlankNodes(false);
						datablock = deleteDataExpr.getDataBlock();
					}

					if (!datablock.equals("")) {
						parser.parse(new StringReader(datablock), "");
					}

					update.addUpdateExpr(updateExpr);

					// associate updateExpr with the correct dataset (if any)
					Dataset dataset = DatasetDeclProcessor.process(uc);
					update.map(updateExpr, dataset);
				}
			} // end for

			return update;
		} catch (RDFParseException | ParseException | TokenMgrError | VisitorException | IOException e) {
			throw new MalformedQueryException(e.getMessage(), e);
		}

	}

	@Override
	public ParsedQuery parseQuery(String queryStr, String baseURI) throws MalformedQueryException {
		try {
			ASTQueryContainer qc = SyntaxTreeBuilder.parseQuery(queryStr);
			StringEscapesProcessor.process(qc);
			BaseDeclProcessor.process(qc, baseURI);
			Map<String, String> prefixes = PrefixDeclProcessor.process(qc, customPrefixes);
			WildcardProjectionProcessor.process(qc);
			BlankNodeVarProcessor.process(qc);

			if (qc.containsQuery()) {

				// handle query operation

				TupleExpr tupleExpr = buildQueryModel(qc);

				// Ensure we always return a rooted query.
				if (!(tupleExpr instanceof QueryRoot)) {
					tupleExpr = new ExtendedQueryRoot(tupleExpr);
				}

				ParsedQuery query;

				ASTQuery queryNode = qc.getQuery();
				if (queryNode instanceof ASTSelectQuery) {
					query = new ParsedTupleQuery(queryStr, tupleExpr);
				} else if (queryNode instanceof ASTConstructQuery) {
					query = new ParsedGraphQuery(queryStr, tupleExpr, prefixes);
				} else if (queryNode instanceof ASTAskQuery) {
					query = new ParsedBooleanQuery(queryStr, tupleExpr);
				} else if (queryNode instanceof ASTDescribeQuery) {
					query = new ParsedDescribeQuery(queryStr, tupleExpr, prefixes);
				} else {
					throw new RuntimeException("Unexpected query type: " + queryNode.getClass());
				}

				// Handle dataset declaration
				Dataset dataset = DatasetDeclProcessor.process(qc);
				if (dataset != null) {
					query.setDataset(dataset);
				}

				return query;
			} else {
				throw new IncompatibleOperationException("supplied string is not a query operation");
			}
		} catch (ParseException | TokenMgrError e) {
			throw new MalformedQueryException(e.getMessage(), e);
		}
	}

	private TupleExpr buildQueryModel(Node qc) throws MalformedQueryException {
		TupleExprBuilder tupleExprBuilder = new TupleExprBuilder(valueFactory);
		try {
			return (TupleExpr) qc.jjtAccept(tupleExprBuilder, null);
		} catch (VisitorException e) {
			throw new MalformedQueryException(e.getMessage(), e);
		}
	}
}
