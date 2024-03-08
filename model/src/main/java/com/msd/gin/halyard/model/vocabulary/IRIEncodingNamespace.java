package com.msd.gin.halyard.model.vocabulary;

import java.nio.ByteBuffer;

public interface IRIEncodingNamespace {
	String getName();
	ByteBuffer writeBytes(String localName, ByteBuffer b);
	String readBytes(ByteBuffer b);
}
