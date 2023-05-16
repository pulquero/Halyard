/*
 * Copyright 2018 Merck Sharp & Dohme Corp. a subsidiary of Merck & Co.,
 * Inc., Kenilworth, NJ, USA.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.msd.gin.halyard.optimizers;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.msd.gin.halyard.algebra.AbstractExtendedQueryModelVisitor;
import com.msd.gin.halyard.algebra.Algebra;
import com.msd.gin.halyard.algebra.BGPCollector;
import com.msd.gin.halyard.algebra.Parent;
import com.msd.gin.halyard.algebra.StarJoin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Triple;
import org.eclipse.rdf4j.common.exception.RDF4JException;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;

public class StarJoinOptimizer implements QueryOptimizer {
	private final int minJoins;

	public StarJoinOptimizer(int minJoins) {
		if (minJoins < 1) {
			throw new IllegalArgumentException("Minimum joins must be greater than or equal to one");
		}
		this.minJoins = minJoins;
	}

	@Override
	public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
		tupleExpr.visit(new StarJoinFinder());
	}

	final class StarJoinFinder extends AbstractExtendedQueryModelVisitor<RDF4JException> {

		@Override
		public void meet(StatementPattern node) {
			// skip children
		}

		@Override
		public void meet(Join node) throws RDF4JException {
			BGPCollector<RDF4JException> collector = new BGPCollector<>(this);
			node.visit(collector);
			if(!collector.getStatementPatterns().isEmpty()) {
				Parent parent = Parent.wrap(node);
				processJoins(parent, collector.getStatementPatterns());
			}
		}

		private void processJoins(Parent parent, List<StatementPattern> sps) {
			ListMultimap<Triple<StatementPattern.Scope,Var,Var>, StatementPattern> spByCtxSubj = ArrayListMultimap.create(sps.size(), 4);
			for(StatementPattern sp : sps) {
				Var ctxVar = sp.getContextVar();
				Var subjVar = sp.getSubjectVar();
				spByCtxSubj.put(Triple.of(sp.getScope(), ctxVar, subjVar), sp);
			}
			List<StarJoin> starJoins = new ArrayList<>(sps.size());
			for(Map.Entry<Triple<StatementPattern.Scope,Var,Var>, Collection<StatementPattern>> entry : spByCtxSubj.asMap().entrySet()) {
				List<StatementPattern> subjSps = (List<StatementPattern>) entry.getValue();
				// (num of joins) = (num of statement patterns) - 1
				if(subjSps.size() > minJoins) {
					for(StatementPattern sp : subjSps) {
						Algebra.remove(sp);
					}
					StatementPattern.Scope scope = entry.getKey().getLeft();
					Var ctxVar = entry.getKey().getMiddle();
					Var commonVar = entry.getKey().getRight();
					starJoins.add(new StarJoin(scope, commonVar.clone(), ctxVar != null ? ctxVar.clone() : null, subjSps));
				}
			}

			if (!starJoins.isEmpty()) {
				Join combined = new Join();
				parent.replaceWith(combined);
				TupleExpr starJoinTree = Algebra.join(starJoins);
				combined.setLeftArg(parent.getArg());
				combined.setRightArg(starJoinTree);
			} else {
				parent.replaceWith(parent.getArg());
			}
		}
	}
}
