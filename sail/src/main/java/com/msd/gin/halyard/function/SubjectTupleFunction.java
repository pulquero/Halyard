package com.msd.gin.halyard.function;

import com.msd.gin.halyard.common.HalyardTableUtils;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Table;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunction;
import org.kohsuke.MetaInfServices;

@MetaInfServices(TupleFunction.class)
public final class SubjectTupleFunction extends AbstractReificationTupleFunction {

	@Override
	public String getURI() {
		return RDF.SUBJECT.stringValue();
	}

	@Override
	protected int statementPosition() {
		return 0;
	}

	@Override
	protected Value getValue(Table table, byte[] id, ValueFactory vf) throws IOException {
		return HalyardTableUtils.getSubject(table, id, vf);
	}
}
