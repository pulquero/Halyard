package com.msd.gin.halyard.common;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Table;

public interface TableProvider {
	Table getTable() throws IOException;
}
