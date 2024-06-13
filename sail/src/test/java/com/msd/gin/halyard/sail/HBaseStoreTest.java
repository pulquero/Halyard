package com.msd.gin.halyard.sail;

import com.msd.gin.halyard.common.HBaseServerTestInstance;

import org.apache.hadoop.conf.Configuration;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.testsuite.sail.RDFStoreTest;
import org.junit.Ignore;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class HBaseStoreTest extends RDFStoreTest {
	private static int uid = 1;

	private static Configuration conf;

	@BeforeAll
	public static void init() throws Exception {
		conf = HBaseServerTestInstance.getInstanceConfig();
	}

	@Override
	protected Sail createSail() {
		Sail sail = new HBaseSail(conf, "storetesttable" + uid++, true, 0, true, 5, null, null);
		sail.init();
		return sail;
	}

	@Override
	@Test
	@Ignore // we return the canonical value
	public void testTimeZoneRoundTrip() {
	}
}
