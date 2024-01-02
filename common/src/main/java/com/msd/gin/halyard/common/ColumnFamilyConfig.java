package com.msd.gin.halyard.common;

import java.util.Arrays;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.BloomFilterUtil;
import org.apache.hadoop.hbase.util.Bytes;

public final class ColumnFamilyConfig {
	public static final String MAX_VERSIONS = "halyard.maxVersions";
	public static final String BLOOM_FILTER_PREFIX_LENGTH = "halyard.bloomFilter.prefixLength";

	static final byte[] CF_NAME = Bytes.toBytes("e");

	private static final int DEFAULT_MAX_VERSIONS = 1;
	private static final Compression.Algorithm DEFAULT_COMPRESSION_ALGORITHM = Compression.Algorithm.GZ;
	private static final DataBlockEncoding DEFAULT_DATABLOCK_ENCODING = DataBlockEncoding.PREFIX;

	static ColumnFamilyDescriptor createColumnFamilyDesc(Configuration conf) {
		int maxVersions = conf.getInt(MAX_VERSIONS, DEFAULT_MAX_VERSIONS);
		ColumnFamilyDescriptorBuilder builder = ColumnFamilyDescriptorBuilder.newBuilder(CF_NAME)
                .setMaxVersions(maxVersions)
                .setBlockCacheEnabled(true)
                .setCompressionType(DEFAULT_COMPRESSION_ALGORITHM)
                .setDataBlockEncoding(DEFAULT_DATABLOCK_ENCODING)
                .setCacheDataOnWrite(true)
                .setCacheIndexesOnWrite(true)
                .setKeepDeletedCells(maxVersions > 1 ? KeepDeletedCells.TRUE : KeepDeletedCells.FALSE);

		RDFFactory rdfFactory = RDFFactory.create(conf);
		int defaultPrefixLength = 1 + Collections.min(Arrays.asList(
			rdfFactory.getSubjectRole(StatementIndex.Name.SPO).keyHashSize() + rdfFactory.getPredicateRole(StatementIndex.Name.SPO).keyHashSize(),
			rdfFactory.getPredicateRole(StatementIndex.Name.POS).keyHashSize() + rdfFactory.getObjectRole(StatementIndex.Name.POS).keyHashSize(),
			rdfFactory.getObjectRole(StatementIndex.Name.OSP).keyHashSize() + rdfFactory.getSubjectRole(StatementIndex.Name.OSP).keyHashSize()
		));
		int bloomFilterPrefixLength = conf.getInt(BLOOM_FILTER_PREFIX_LENGTH, defaultPrefixLength);
		if (bloomFilterPrefixLength > 0) {
            builder.setBloomFilterType(BloomType.ROWPREFIX_FIXED_LENGTH)
            .setConfiguration(BloomFilterUtil.PREFIX_LENGTH_KEY, Integer.toString(bloomFilterPrefixLength))
            .setCacheBloomsOnWrite(true);
		} else {
			builder.setBloomFilterType(BloomType.NONE);
		}

		return builder.build();
    }

	private ColumnFamilyConfig() {}
}
