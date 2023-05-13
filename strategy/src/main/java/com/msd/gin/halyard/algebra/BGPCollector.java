package com.msd.gin.halyard.algebra;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.QueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;

/**
 * Basic graph pattern collector.
 */
public class BGPCollector<X extends Exception> extends AbstractExtendedQueryModelVisitor<X> {

	private final QueryModelVisitor<X> visitor;

	private List<StatementPattern> statementPatterns;

	public BGPCollector(QueryModelVisitor<X> visitor) {
		this.visitor = visitor;
	}

	public List<StatementPattern> getStatementPatterns() {
		return (statementPatterns != null) ? statementPatterns : Collections.<StatementPattern>emptyList();
	}

	@Override
	public void meet(Join node) throws X {
		// by-pass meetNode()
		node.visitChildren(this);
	}

	@Override
	public void meet(StatementPattern sp) throws X {
		if (statementPatterns == null) {
			statementPatterns = new ArrayList<>();
		}
		statementPatterns.add(sp);
	}

	@Override
	public void meet(StarJoin node) throws X {
		// by-pass meetNode()
		for (TupleExpr arg : node.getArgs()) {
			arg.visit(this);
		}
	}

	@Override
	protected void meetNode(QueryModelNode node) throws X {
		// resume previous visitor
		node.visit(visitor);
	}
}
