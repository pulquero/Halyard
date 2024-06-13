package com.msd.gin.halyard.sail;

import com.msd.gin.halyard.common.RDFContext;
import com.msd.gin.halyard.common.RDFObject;
import com.msd.gin.halyard.common.RDFPredicate;
import com.msd.gin.halyard.common.RDFSubject;
import com.msd.gin.halyard.common.StatementIndices;

import java.util.NoSuchElementException;

import org.apache.hadoop.hbase.client.Result;
import org.eclipse.rdf4j.common.iteration.AbstractCloseableIteration;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;

public abstract class AbstractStatementScanner extends AbstractCloseableIteration<Statement> {
	protected final StatementIndices indices;
	protected final ValueFactory vf;
	protected RDFSubject subj;
	protected RDFPredicate pred;
	protected RDFObject obj;
	protected RDFContext ctx;
	private Statement next = null;
	private Statement[] stmts = null;
	private int stmtIndex = 0;
	private int stmtLength = 0;

	protected AbstractStatementScanner(StatementIndices indices, ValueFactory vf) {
		this.indices = indices;
		this.vf = vf;
	}

	protected abstract Result nextResult();

	@Override
	public final boolean hasNext() {
		if (next == null) {
			while (true) {
				if (stmts == null) {
					Result res = nextResult();
					if (res == null) {
						return false; // no more Results
					}
					stmts = indices.parseStatements(subj, pred, obj, ctx, res, vf);
					stmtIndex = 0;
					stmtLength = stmts.length;
				}
				if (stmtIndex < stmtLength) {
					next = stmts[stmtIndex++]; // cache the next statement which will be returned with a call to next().
					return true; // there is another statement
				}
				stmts = null;
			}
		} else {
			return true;
		}
	}

	@Override
	public final Statement next() {
		if (!hasNext()) {
			throw new NoSuchElementException();
		}
		// return the next statement and set next to null so it can be refilled by the next call to hasNext()
		Statement st = next;
		next = null;
		return st;
	}

	@Override
	public final void remove() {
		throw new UnsupportedOperationException();
	}
}
