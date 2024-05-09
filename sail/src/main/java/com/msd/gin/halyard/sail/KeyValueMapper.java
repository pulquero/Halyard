package com.msd.gin.halyard.sail;

import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;

@FunctionalInterface
public interface KeyValueMapper {
	List<? extends KeyValue> toKeyValues(Resource subj, IRI pred, Value obj, Resource context, long timestamp);
}
