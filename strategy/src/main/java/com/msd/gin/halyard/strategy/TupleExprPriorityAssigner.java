package com.msd.gin.halyard.strategy;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.msd.gin.halyard.query.algebra.AbstractExtendedQueryModelVisitor;
import com.msd.gin.halyard.query.algebra.ServiceRoot;

import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.LeftJoin;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class TupleExprPriorityAssigner {
	private static final Logger LOGGER = LoggerFactory.getLogger(TupleExprPriorityAssigner.class);

	// high default priority for dynamically created query nodes
	private static final int DEFAULT_PRIORITY = 65535;
	// a map of query model nodes and their priority
	private final Cache<TupleExpr, Integer> priorityMapCache = Caffeine.newBuilder().weakKeys().build();

	/**
     * Get the priority of this node from the PRIORITY_MAP_CACHE or determine the priority and then cache it. Also caches priority for sub-nodes of {@code node}
     * @param node the node that you want the priority for
     * @return the priority of the node.
     */
    int getPriority(final TupleExpr node) {
        Integer p = priorityMapCache.getIfPresent(node);
        if (p != null) {
            return p;
        } else {
            QueryModelNode root = node;
            while (root.getParentNode() != null) {
            	root = root.getParentNode(); //traverse to the root of the query model
            }
            // while we have a strong ref to the root node, none of the child node keys should be gc-ed

            //starting priority for ServiceRoot must be evaluated from the original service args node
            int startingPriority = (root instanceof ServiceRoot) ? getPriority(((ServiceRoot)root).getService().getArg()) - 1 : 0;
            final AtomicInteger counter = new AtomicInteger(startingPriority);

            // populate the priority cache
            new AbstractExtendedQueryModelVisitor<RuntimeException>() {
            	private int setPriority(TupleExpr n) {
                    int pp = counter.getAndIncrement();
                    priorityMapCache.put(n, pp);
                    return pp;
            	}

            	@Override
                protected void meetNode(QueryModelNode n) {
            		if (n instanceof TupleExpr) {
            			setPriority((TupleExpr) n);
            		}
                    n.visitChildren(this);
                }

    			@Override
    			public void meet(StatementPattern node) {
    				setPriority(node);
    				// skip children
    			}

                @Override
                public void meet(Filter node) {
                    super.meet(node);
                    node.getCondition().visit(this);
                }

                @Override
                public void meet(Service n) {
                	int pp = setPriority(n);
                    n.visitChildren(this);
                    counter.getAndUpdate((int count) -> 2 * count - pp + 1); //at least double the distance to have a space for service optimizations
                }

                @Override
                public void meet(LeftJoin node) {
                    super.meet(node);
                    if (node.hasCondition()) {
                        meetNode(node.getCondition());
                    }
                }
            }.meetOther(root);

            Integer priority = priorityMapCache.getIfPresent(node);
            if (priority == null) {
                // else node is dynamically created, so climb the tree to find an ancestor with a priority
                QueryModelNode parent = node.getParentNode();
                int depth = 1;
                while (parent != null && (priority = priorityMapCache.getIfPresent(parent)) == null) {
                    parent = parent.getParentNode();
                    depth++;
                }
                if (priority != null) {
                    priority = priority + depth;
                }
            }
            if (priority == null) {
                LOGGER.warn("Failed to ascertain a priority for node\n{}\n with root\n{}\n - using default value {}", node, root, DEFAULT_PRIORITY);
                // else fallback to a default value
                priority = DEFAULT_PRIORITY;
            }
            return priority;
        }
    }
}
