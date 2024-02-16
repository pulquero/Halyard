/*
 * Copyright 2016 Merck Sharp & Dohme Corp. a subsidiary of Merck & Co.,
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
package com.msd.gin.halyard.query;

import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;

/**
 * Binding set pipes instances hold {@link BindingSet}s (set of evaluated, un-evaluated and intermediate variables) that
 * form part of the query evaluation (a query generates an evaluation tree).
 */
public abstract class BindingSetPipe {
	private static final int ACCEPT_STATE = 0;
	private static final int REJECT_STATE = 1;
	private static final int CLOSED_ONCE_STATE = 2;
	private static final int CLOSED_MANY_STATE = 3;

    protected final BindingSetPipe parent;
    private final AtomicInteger state = new AtomicInteger(ACCEPT_STATE);

    /**
     * Create a pipe
     * @param parent the parent of this part of the evaluation chain
     */
    protected BindingSetPipe(BindingSetPipe parent) {
        this.parent = parent;
    }

    /**
     * Pushes BindingSet up the pipe, use pushLast() to indicate end of data. In case you need to interrupt the tree data flow
     * (when for example just a Slice of data is expected), it is necessary to indicate that no more data is expected down the tree
     * (to stop feeding this pipe) by returning false and
     * also to indicate up the tree that this is the end of data
     * (by calling pushLast() on the parent pipe in the evaluation tree).
     *
     * @param bs BindingSet
     * @return boolean indicating if more data is expected from the caller
     */
    public final boolean push(BindingSet bs) {
    	if (state.get() == ACCEPT_STATE) {
    		boolean pushMore = next(bs);
    		if (!pushMore) {
    			state.set(REJECT_STATE);
    		}
    		return pushMore;
    	} else {
    		return false;
    	}
    }

    /**
     * Must be thread-safe.
     * 
     * @param bs BindingSet
     * @return boolean indicating if more data is expected from the caller
     */
    protected boolean next(BindingSet bs) {
    	if (parent != null) {
    		return parent.push(bs);
    	} else {
    		return true;
    	}
    }

    public final void close() {
    	if (state.updateAndGet(current -> {
    		if (current < CLOSED_ONCE_STATE) {
    			return CLOSED_ONCE_STATE;
    		} else {
    			return CLOSED_MANY_STATE;
    		}
    	}) == CLOSED_ONCE_STATE) {
    		doClose();
    	}
    }

    protected void doClose() {
    	if (parent != null) {
    		parent.close();
    	}
    }

    public final void pushLast(BindingSet bs) {
    	push(bs);
    	close();
    }

    public boolean handleException(Throwable e) {
        if (parent != null) {
            return parent.handleException(e);
        } else if (e instanceof QueryEvaluationException) {
        	throw (QueryEvaluationException) e;
        } else {
        	throw new QueryEvaluationException(e);
        }
    }

    public final boolean isClosed() {
    	return state.get() >= CLOSED_ONCE_STATE;
    }
}
