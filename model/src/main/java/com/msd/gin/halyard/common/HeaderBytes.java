package com.msd.gin.halyard.common;

final class HeaderBytes {
	private HeaderBytes() {}

	// reserved for method return values
	static final byte RESERVED_TYPE = 0;
	static final byte IRI_TYPE = '<';
	static final byte COMPRESSED_IRI_TYPE = 'w';
	static final byte IRI_HASH_TYPE = '#';
	static final byte NAMESPACE_HASH_TYPE = ':';
	static final byte ENCODED_IRI_TYPE = '{';
	static final byte END_SLASH_ENCODED_IRI_TYPE = '}';
	static final byte BNODE_TYPE = '_';
	static final byte DATATYPE_LITERAL_TYPE = '\"';
	static final byte LANGUAGE_LITERAL_TYPE = '@';
	static final byte TRIPLE_TYPE = '*';
	static final byte FALSE_TYPE = '0';
	static final byte TRUE_TYPE = '1';
	static final byte LANGUAGE_HASH_LITERAL_TYPE = 'a';
	static final byte BYTE_TYPE = 'b';
	static final byte SHORT_TYPE = 's';
	static final byte INT_TYPE = 'i';
	static final byte LONG_TYPE = 'l';
	static final byte FLOAT_TYPE = 'f';
	static final byte DOUBLE_TYPE = 'd';
	static final byte COMPRESSED_STRING_TYPE = 'z';
	static final byte UNCOMPRESSED_STRING_TYPE = 'Z';
	static final byte SCSU_STRING_TYPE = 'U';
	static final byte TIME_TYPE = 't';
	static final byte DATE_TYPE = 'D';
	static final byte BIG_FLOAT_TYPE = 'F';
	static final byte BIG_INT_TYPE = 'I';
	static final byte LONG_COMPRESSED_BIG_INT_TYPE = '8';
	static final byte INT_COMPRESSED_BIG_INT_TYPE = '4';
	static final byte SHORT_COMPRESSED_BIG_INT_TYPE = '2';
	static final byte DATETIME_TYPE = 'T';
	static final byte WKT_LITERAL_TYPE = 'W';
	static final byte XML_TYPE = 'x';
}
