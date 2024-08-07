package com.msd.gin.halyard.common;

import com.google.common.collect.Sets;
import com.ibm.icu.text.UnicodeCompressor;
import com.ibm.icu.text.UnicodeDecompressor;
import com.msd.gin.halyard.model.ValueType;
import com.msd.gin.halyard.model.WKTLiteral;
import com.msd.gin.halyard.model.XMLLiteral;
import com.msd.gin.halyard.model.vocabulary.IRIEncodingNamespace;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.BiFunction;

import javax.annotation.concurrent.ThreadSafe;
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeConstants;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import javax.xml.transform.TransformerException;

import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Triple;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.model.vocabulary.GEO;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.locationtech.jts.io.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

public class ValueIO {
	public static final int DEFAULT_BUFFER_SIZE = 256;

	private static final Logger LOGGER = LoggerFactory.getLogger(ValueIO.class);
	private static final int IRI_HASH_SIZE = 4;
	private static final int NAMESPACE_HASH_SIZE = 2;
	private static final int LANG_HASH_SIZE = 2;
	private static final Set<String> SCSU_OPTIMAL_LANGS = Sets.newHashSet(
		"ar", "arz", "azb", "ba", "be", "bg", "bn", "ckb", "dty", "dv", "el", "fa","gan", "gu", "he", "hi", "hy", "ja",
		"ka", "kk", "kn", "ko", "ky", "mk", "ml", "mn", "my", "or",
		"ne", "ps", "ru", "sd", "sh", "sr", "ta", "tg", "th", "tt", "uk", "ur", "vi", "wuu", "yue", "zh"
	);
	private static final BiFunction<String,ValueFactory,Resource> DEFAULT_BNODE_TRANSFORMER = (id,valueFactory) -> valueFactory.createBNode(id);

	interface ByteWriter {
		ByteBuffer writeBytes(Literal l, ByteBuffer b);
	}

	interface ByteReader {
		Literal readBytes(ByteBuffer b, ValueFactory vf);
	}

	static final DatatypeFactory DATATYPE_FACTORY;
	static {
		try {
			DATATYPE_FACTORY = DatatypeFactory.newInstance();
		} catch (DatatypeConfigurationException e) {
			throw new AssertionError(e);
		}
	}

	private static final LZ4Factory LZ4 = LZ4Factory.fastestJavaInstance();
	private static final Date GREGORIAN_ONLY = new Date(Long.MIN_VALUE);

	private static GregorianCalendar newGregorianCalendar() {
		GregorianCalendar cal = new GregorianCalendar();
		cal.setGregorianChange(GREGORIAN_ONLY);
		return cal;
	}

	public static XMLGregorianCalendar parseCalendar(String label) {
		XMLGregorianCalendar cal = DATATYPE_FACTORY.newXMLGregorianCalendar(label);
		BigDecimal frac = cal.getFractionalSecond();
		if (frac != null) {
			cal.setFractionalSecond(frac.setScale(3));
		}
		return cal;
	}

	public static boolean parseBoolean(String label) {
		label = label.trim();
		if ("true".equals(label) || "1".equals(label)) {
			return true;
		} else if ("false".equals(label) || "0".equals(label)) {
			return false;
		} else {
			throw new IllegalArgumentException("Invalid boolean value");
		}
	}


	// compressed IRI types
	private static final byte HTTP_SCHEME = 'h';
	private static final byte HTTPS_SCHEME = 's';
	private static final byte DOI_HTTP_SCHEME = 'd';
	private static final byte DOI_HTTPS_SCHEME = 'D';

	private static ByteBuffer writeCalendar(byte type, XMLGregorianCalendar cal, ByteBuffer b) {
		long subMillis;
		int extraBytes;
		BigDecimal fracSecs = cal.getFractionalSecond();
		boolean hasMillis = (fracSecs != null);
		if (hasMillis) {
			if (fracSecs.scale() > 3) {
				// if there is more than millisecond accuracy then store it
				BigDecimal fracMillis = fracSecs.scaleByPowerOfTen(3);
				subMillis = fracMillis.subtract(new BigDecimal(fracMillis.toBigInteger())).scaleByPowerOfTen(6).longValue();
				extraBytes = Long.BYTES;
			} else {
				subMillis = -1L;
				extraBytes = Byte.BYTES;
			}
		} else {
			subMillis = -1L;
			extraBytes = 0;
		}
		b = ByteUtils.ensureCapacity(b, 1 + Long.BYTES + extraBytes + Short.BYTES);
		b.put(type).putLong(cal.toGregorianCalendar().getTimeInMillis());
		if (subMillis > 0L) {
			b.putLong(subMillis);
		} else if (hasMillis) {
			// write byte to indicate millis are significant
			b.put((byte) 0);
		}
		if(cal.getTimezone() != DatatypeConstants.FIELD_UNDEFINED) {
			b.putShort((short) cal.getTimezone());
		}
		return b;
	}

	private static XMLGregorianCalendar readCalendar(ByteBuffer b) {
		long ts = b.getLong();
		int rem = b.remaining();
		boolean hasMillis = (rem == Byte.BYTES || rem == Byte.BYTES + Short.BYTES);
		if (hasMillis) {
			b.get();
		}
		long subMillis = (rem == Long.BYTES || rem == Long.BYTES + Short.BYTES) ? b.getLong() : -1L;
		int tz = (b.remaining() == 2) ? b.getShort() : Short.MIN_VALUE;
		GregorianCalendar c = newGregorianCalendar();
		if (tz != Short.MIN_VALUE) {
			int tzHr = tz/60;
			int tzMin = tz - 60 * tzHr;
			c.setTimeZone(TimeZone.getTimeZone(String.format("GMT%+02d%02d", tzHr, tzMin)));
		}
		c.setTimeInMillis(ts);
		XMLGregorianCalendar cal = DATATYPE_FACTORY.newXMLGregorianCalendar(c);
		if (subMillis > 0L) {
			BigDecimal fracSecs = cal.getFractionalSecond().add(BigDecimal.valueOf(subMillis, 9)).stripTrailingZeros();
			cal.setFractionalSecond(fracSecs);
		} else if (!hasMillis) {
			cal.setFractionalSecond(null);
		}
		if (tz == Short.MIN_VALUE) {
			cal.setTimezone(DatatypeConstants.FIELD_UNDEFINED);
		}
		return cal;
	}

	private static String readString(ByteBuffer b) {
		int type = b.get();
		switch (type) {
			case HeaderBytes.UNCOMPRESSED_STRING_TYPE:
				return readUncompressedString(b);
			case HeaderBytes.COMPRESSED_STRING_TYPE:
				return readCompressedString(b);
			case HeaderBytes.SCSU_STRING_TYPE:
				return readScsuString(b);
			default:
				throw new AssertionError(String.format("Unrecognized string type: %d", type));
		}
	}

	public static ByteBuffer writeUncompressedString(String s) {
		return StandardCharsets.UTF_8.encode(s);
	}

	public static String readUncompressedString(ByteBuffer b) {
		return StandardCharsets.UTF_8.decode(b).toString();
	}

	private static ByteBuffer writeCompressedString(String s, ByteBuffer b) {
		ByteBuffer uncompressed = writeUncompressedString(s);
		int uncompressedLen = uncompressed.remaining();
		ByteBuffer compressed = compress(uncompressed);
		b = ByteUtils.ensureCapacity(b, Integer.BYTES + compressed.remaining());
		return b.putInt(uncompressedLen).put(compressed);
	}

	private static String readCompressedString(ByteBuffer b) {
		ByteBuffer uncompressed = decompress(b);
		return readUncompressedString(uncompressed);
	}

	private static ByteBuffer compress(ByteBuffer b) {
		LZ4Compressor compressor = LZ4.fastCompressor();
		int maxLen = compressor.maxCompressedLength(b.remaining());
		ByteBuffer compressed = ByteBuffer.allocate(maxLen);
		compressor.compress(b, compressed);
		b.flip();
		compressed.flip();
		return compressed;
	}

	private static ByteBuffer decompress(ByteBuffer b) {
		LZ4FastDecompressor decompressor = LZ4.fastDecompressor();
		int len = b.getInt();
		ByteBuffer uncompressed = ByteBuffer.allocate(len);
		decompressor.decompress(b, uncompressed);
		uncompressed.flip();
		return uncompressed;
	}

	private static ByteBuffer writeScsuString(String s, ByteBuffer b) {
		char[] chars = s.toCharArray();
		int len = chars.length;
		UnicodeCompressor compressor = new UnicodeCompressor();
		int[] charsReadHolder = new int[1];
		int totalCharsRead = 0;
		do {
			// ensure we have a decent amount of available buffer to write to
			b = ByteUtils.ensureCapacity(b, 512);
			int pos = b.position();
			int bytesWritten = compressor.compress(chars, totalCharsRead, len, charsReadHolder, b.array(), b.arrayOffset() + pos, b.remaining());
			b.position(pos + bytesWritten);
			totalCharsRead += charsReadHolder[0];
		} while (totalCharsRead < len);
		return b;
	}

	private static String readScsuString(ByteBuffer b) {
		byte[] bytes = b.array();
		int offset = b.arrayOffset();
		int len = b.limit();
		CharBuffer c = CharBuffer.allocate(512);
		UnicodeDecompressor decompressor = new UnicodeDecompressor();
		int[] bytesReadHolder = new int[1];
		do {
			// ensure we have a decent amount of available buffer to write to
			c = ensureCapacity(c, 512);
			int bpos = b.position();
			int cpos = c.position();
			int charsWritten = decompressor.decompress(bytes, offset + bpos, len, bytesReadHolder, c.array(), c.arrayOffset() + cpos, c.remaining());
			b.position(bpos + bytesReadHolder[0]);
			c.position(cpos + charsWritten);
		} while (b.hasRemaining());
		c.flip();
		return c.toString();
	}

	public static CharBuffer ensureCapacity(CharBuffer c, int requiredSize) {
		if (c.remaining() < requiredSize) {
			// leave some spare capacity
			CharBuffer newc = CharBuffer.allocate(3*c.capacity()/2 + 2*requiredSize);
			c.flip();
			newc.put(c);
			return newc;
		} else {
			return c;
		}
	}

	private static volatile ValueIO defaultValueIO;
	private static volatile ValueIO.Writer defaultWriter;
	private static volatile ValueIO.Reader defaultReader;

	public static ValueIO getDefault() {
		if (defaultValueIO == null) {
			synchronized (ValueIO.class) {
				if (defaultValueIO == null) {
					ValueIO valueIO = new ValueIO(new HalyardTableConfiguration((Iterable<Map.Entry<String, String>>) (Iterable<?>) System.getProperties().entrySet()));
					defaultWriter = valueIO.createWriter();
					defaultReader = valueIO.createReader();
					defaultValueIO = valueIO;
				}
			}
		}
		return defaultValueIO;
	}

	public static ValueIO.Writer getDefaultWriter() {
		getDefault();
		return defaultWriter;
	}

	public static ValueIO.Reader getDefaultReader() {
		getDefault();
		return defaultReader;
	}

	private final HalyardTableConfiguration config;
	private final Map<CoreDatatype, ByteWriter> byteWriters = new IdentityHashMap<>(32);
	private final Map<Integer, ByteReader> byteReaders = new HashMap<>(32);
	private final int stringCompressionThreshold;

	public ValueIO(HalyardTableConfiguration config) {
		this.config = config;
		int stringCompressionThreshold = config.getInt(TableConfig.STRING_COMPRESSION);
		if (stringCompressionThreshold < 0) {
			throw new IllegalArgumentException("String compression threshold must be greater than or equal to zero");
		}
		this.stringCompressionThreshold = stringCompressionThreshold;

		addByteReaderWriters();
	}

	private void addByteWriter(CoreDatatype datatype, ByteWriter bw) {
		if (byteWriters.putIfAbsent(datatype, bw) != null) {
			throw new AssertionError(String.format("%s already exists for %s", ByteWriter.class.getSimpleName(), datatype));
		}
	}

	private void addByteReader(int valueType, ByteReader br) {
		if (byteReaders.putIfAbsent(valueType, br) != null) {
			throw new AssertionError(String.format("%s already exists for %s", ByteReader.class.getSimpleName(), (char)valueType));
		}
	}

	private void addByteReaderWriters() {
		addByteWriter(CoreDatatype.XSD.BOOLEAN, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				boolean v = l.booleanValue();
				b = ByteUtils.ensureCapacity(b, 1);
				return b.put(v ? HeaderBytes.TRUE_TYPE : HeaderBytes.FALSE_TYPE);
			}
		});
		addByteReader(HeaderBytes.FALSE_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				return vf.createLiteral(false);
			}
		});
		addByteReader(HeaderBytes.TRUE_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				return vf.createLiteral(true);
			}
		});

		addByteWriter(CoreDatatype.XSD.BYTE, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				byte v = l.byteValue();
				return ByteUtils.ensureCapacity(b, 1 + Byte.BYTES).put(HeaderBytes.BYTE_TYPE).put(v);
			}
		});
		addByteReader(HeaderBytes.BYTE_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				return vf.createLiteral(b.get());
			}
		});

		addByteWriter(CoreDatatype.XSD.SHORT, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				short v = l.shortValue();
				return ByteUtils.ensureCapacity(b, 1 + Short.BYTES).put(HeaderBytes.SHORT_TYPE).putShort(v);
			}
		});
		addByteReader(HeaderBytes.SHORT_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				return vf.createLiteral(b.getShort());
			}
		});

		addByteWriter(CoreDatatype.XSD.INT, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				int v = l.intValue();
				return ByteUtils.ensureCapacity(b, 1 + Integer.BYTES).put(HeaderBytes.INT_TYPE).putInt(v);
			}
		});
		addByteReader(HeaderBytes.INT_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				return vf.createLiteral(b.getInt());
			}
		});

		addByteWriter(CoreDatatype.XSD.LONG, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				long v = l.longValue();
				return ByteUtils.ensureCapacity(b, 1 + Long.BYTES).put(HeaderBytes.LONG_TYPE).putLong(v);
			}
		});
		addByteReader(HeaderBytes.LONG_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				return vf.createLiteral(b.getLong());
			}
		});

		addByteWriter(CoreDatatype.XSD.FLOAT, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				float v = l.floatValue();
				return ByteUtils.ensureCapacity(b, 1 + Float.BYTES).put(HeaderBytes.FLOAT_TYPE).putFloat(v);
			}
		});
		addByteReader(HeaderBytes.FLOAT_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				return vf.createLiteral(b.getFloat());
			}
		});

		addByteWriter(CoreDatatype.XSD.DOUBLE, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				double v = l.doubleValue();
				return ByteUtils.ensureCapacity(b, 1 + Double.BYTES).put(HeaderBytes.DOUBLE_TYPE).putDouble(v);
			}
		});
		addByteReader(HeaderBytes.DOUBLE_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				return vf.createLiteral(b.getDouble());
			}
		});

		addByteWriter(CoreDatatype.XSD.INTEGER, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				BigInteger bigInt;
				long x;
				if (l.getLabel().length() < ByteUtils.MAX_LONG_STRING_LENGTH) {
					x = l.longValue();
					bigInt = null;
				} else {
					bigInt = l.integerValue();
					x = 0;
				}
				return ByteUtils.writeCompressedInteger(bigInt, x, b);
			}
		});
		addByteReader(HeaderBytes.BIG_INT_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				byte[] bytes = new byte[b.remaining()];
				b.get(bytes);
				return vf.createLiteral(new BigInteger(bytes));
			}
		});
		addByteReader(HeaderBytes.INT_COMPRESSED_BIG_INT_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				int v = b.getInt();
				if (vf instanceof IdValueFactory) {
					return ((IdValueFactory)vf).createLiteral(v, CoreDatatype.XSD.INTEGER);
				} else {
					return vf.createLiteral(BigInteger.valueOf(v));
				}
			}
		});
		addByteReader(HeaderBytes.SHORT_COMPRESSED_BIG_INT_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				int v = b.getShort();
				if (vf instanceof IdValueFactory) {
					return ((IdValueFactory)vf).createLiteral(v, CoreDatatype.XSD.INTEGER);
				} else {
					return vf.createLiteral(BigInteger.valueOf(v));
				}
			}
		});
		addByteReader(HeaderBytes.LONG_COMPRESSED_BIG_INT_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				long v = b.getLong();
				return vf.createLiteral(BigInteger.valueOf(v));
			}
		});

		addByteWriter(CoreDatatype.XSD.DECIMAL, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				BigDecimal x = l.decimalValue();
				byte[] bytes = x.unscaledValue().toByteArray();
				int scale = x.scale();
				b = ByteUtils.ensureCapacity(b, 1 + Integer.BYTES + bytes.length);
				return b.put(HeaderBytes.BIG_FLOAT_TYPE).putInt(scale).put(bytes);
			}
		});
		addByteReader(HeaderBytes.BIG_FLOAT_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				int scale = b.getInt();
				byte[] bytes = new byte[b.remaining()];
				b.get(bytes);
				return vf.createLiteral(new BigDecimal(new BigInteger(bytes), scale));
			}
		});

		addByteWriter(CoreDatatype.XSD.STRING, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				return writeString(l.getLabel(), b);
			}
		});
		addByteReader(HeaderBytes.COMPRESSED_STRING_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				return vf.createLiteral(readCompressedString(b));
			}
		});
		addByteReader(HeaderBytes.UNCOMPRESSED_STRING_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				return vf.createLiteral(readUncompressedString(b));
			}
		});

		addByteWriter(CoreDatatype.RDF.LANGSTRING, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				String langTag = l.getLanguage().get();
				b = writeLanguagePrefix(langTag, b);
				return writeString(l.getLabel(), b, langTag);
			}
		});
		addByteReader(HeaderBytes.LANGUAGE_HASH_LITERAL_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				b.mark();
				short langHash = b.getShort(); // 16-bit hash
				String label = readString(b);
				String lang = config.getWellKnownLanguageTag(langHash);
				if (lang == null) {
					b.limit(b.position()).reset();
					throw new IllegalStateException(String.format("Unknown language tag hash: %s (%d) (label %s)", ByteUtils.encode(b), langHash, label));
				}
				return vf.createLiteral(label, lang);
			}
		});
		addByteReader(HeaderBytes.LANGUAGE_LITERAL_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				int originalLimit = b.limit();
				int langSize = b.get();
				b.limit(b.position()+langSize);
				String lang = readUncompressedString(b);
				b.limit(originalLimit);
				String label = readString(b);
				return vf.createLiteral(label, lang);
			}
		});

		addByteWriter(CoreDatatype.XSD.TIME, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				return writeCalendar(HeaderBytes.TIME_TYPE, l.calendarValue(), b);
			}
		});
		addByteReader(HeaderBytes.TIME_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				XMLGregorianCalendar cal = readCalendar(b);
				cal.setYear(null);
				cal.setMonth(DatatypeConstants.FIELD_UNDEFINED);
				cal.setDay(DatatypeConstants.FIELD_UNDEFINED);
				return vf.createLiteral(cal);
			}
		});

		addByteWriter(CoreDatatype.XSD.DATE, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				return writeCalendar(HeaderBytes.DATE_TYPE, l.calendarValue(), b);
			}
		});
		addByteReader(HeaderBytes.DATE_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				XMLGregorianCalendar cal = readCalendar(b);
				cal.setHour(DatatypeConstants.FIELD_UNDEFINED);
				cal.setMinute(DatatypeConstants.FIELD_UNDEFINED);
				cal.setSecond(DatatypeConstants.FIELD_UNDEFINED);
				cal.setFractionalSecond(null);
				return vf.createLiteral(cal);
			}
		});

		addByteWriter(CoreDatatype.XSD.DATETIME, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				return writeCalendar(HeaderBytes.DATETIME_TYPE, l.calendarValue(), b);
			}
		});
		addByteReader(HeaderBytes.DATETIME_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				XMLGregorianCalendar cal = readCalendar(b);
				return vf.createLiteral(cal);
			}
		});

		addByteWriter(CoreDatatype.GEO.WKT_LITERAL, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				try {
					byte[] wkb = WKTLiteral.writeWKB(l);
					b = ByteUtils.ensureCapacity(b, 2 + wkb.length);
					b.put(HeaderBytes.WKT_LITERAL_TYPE);
					b.put(HeaderBytes.WKT_LITERAL_TYPE); // mark as valid
					return b.put(wkb);
				} catch (ParseException e) {
					ByteBuffer wktBytes = writeUncompressedString(l.getLabel());
					b = ByteUtils.ensureCapacity(b, 2 + wktBytes.remaining());
					b.put(HeaderBytes.WKT_LITERAL_TYPE);
					b.put(HeaderBytes.UNCOMPRESSED_STRING_TYPE); // mark as invalid
					return b.put(wktBytes);
				}
			}
		});
		addByteReader(HeaderBytes.WKT_LITERAL_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				int wktType = b.get();
				switch (wktType) {
					case HeaderBytes.WKT_LITERAL_TYPE:
						// valid wkt
						byte[] wkbBytes = new byte[b.remaining()];
						b.get(wkbBytes);
						return new WKTLiteral(wkbBytes);
					case HeaderBytes.UNCOMPRESSED_STRING_TYPE:
						// invalid xml
						return vf.createLiteral(readUncompressedString(b), GEO.WKT_LITERAL);
					default:
						throw new AssertionError(String.format("Unrecognized WKT type: %d", wktType));
				}
			}
		});

		addByteWriter(CoreDatatype.RDF.XMLLITERAL, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				try {
					ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
					XMLLiteral.writeInfoset(l.getLabel(), out);
					byte[] xb = out.toByteArray();
					b = ByteUtils.ensureCapacity(b, 2 + xb.length);
					b.put(HeaderBytes.XML_TYPE);
					b.put(HeaderBytes.XML_TYPE); // mark xml as valid
					return b.put(xb);
				} catch(TransformerException e) {
					b = ByteUtils.ensureCapacity(b, 2);
					b.put(HeaderBytes.XML_TYPE);
					b.put(HeaderBytes.COMPRESSED_STRING_TYPE); // mark xml as invalid
					return writeCompressedString(l.getLabel(), b);
				}
			}
		});
		addByteReader(HeaderBytes.XML_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				int xmlType = b.get();
				switch (xmlType) {
					case HeaderBytes.XML_TYPE:
						// valid xml
						byte[] fiBytes = new byte[b.remaining()];
						b.get(fiBytes);
						return new XMLLiteral(fiBytes);
					case HeaderBytes.COMPRESSED_STRING_TYPE:
						// invalid xml
						return vf.createLiteral(readCompressedString(b), RDF.XMLLITERAL);
					default:
						throw new AssertionError(String.format("Unrecognized XML type: %d", xmlType));
				}
			}
		});
	}

	private ByteBuffer writeString(String s, ByteBuffer b, String langTag) {
		int charLen = s.length();
		int estimatedByteSize = Character.BYTES * charLen;
		int sepPos = langTag.indexOf('-');
		String lang = (sepPos != -1) ? langTag.substring(0, sepPos) : langTag;
		if (estimatedByteSize < stringCompressionThreshold && SCSU_OPTIMAL_LANGS.contains(lang)) {
			b.put(HeaderBytes.SCSU_STRING_TYPE);
			return writeScsuString(s, b);
		} else {
			b.put(HeaderBytes.COMPRESSED_STRING_TYPE);
			return writeCompressedString(s, b);
		}
	}

	private ByteBuffer writeString(String s, ByteBuffer b) {
		ByteBuffer uncompressed = writeUncompressedString(s);
		int uncompressedLen = uncompressed.remaining();
		ByteBuffer compressed;
		int compressedLen;
		if (uncompressedLen > stringCompressionThreshold) {
			compressed = compress(uncompressed);
			compressedLen = compressed.remaining();
		} else {
			compressed = null;
			compressedLen = -1;
		}
		if (compressed != null && Integer.BYTES + compressedLen < uncompressedLen) {
			b = ByteUtils.ensureCapacity(b, 1 + Integer.BYTES + compressedLen);
			return b.put(HeaderBytes.COMPRESSED_STRING_TYPE).putInt(uncompressedLen).put(compressed);
		} else {
			if (compressed != null && LOGGER.isDebugEnabled()) {
				LOGGER.debug("Ineffective compression of string of byte length %d", uncompressedLen);
			}
			b = ByteUtils.ensureCapacity(b, 1 + uncompressedLen);
			return b.put(HeaderBytes.UNCOMPRESSED_STRING_TYPE).put(uncompressed);
		}
	}

	private ByteBuffer writeLanguagePrefix(String langTag, ByteBuffer b) {
		Short hash = config.getWellKnownLanguageTagHash(langTag);
		if (hash != null) {
			b = ByteUtils.ensureCapacity(b, 1+LANG_HASH_SIZE);
			b.put(HeaderBytes.LANGUAGE_HASH_LITERAL_TYPE);
			b.putShort(hash);
		} else {
			if (langTag.length() > Short.MAX_VALUE) {
				int truncatePos = langTag.lastIndexOf('-', Short.MAX_VALUE-1);
				// check for single tag
				if (langTag.charAt(truncatePos-2) == '-') {
					truncatePos -= 2;
				}
				langTag = langTag.substring(0, truncatePos);
			}
			ByteBuffer langBytes = writeUncompressedString(langTag);
			b = ByteUtils.ensureCapacity(b, 1+1+langBytes.remaining());
			b.put(HeaderBytes.LANGUAGE_LITERAL_TYPE);
			int langBytesLen = langBytes.remaining();
			assert langBytesLen <= Byte.MAX_VALUE;
			b.put((byte) langBytesLen);
			b.put(langBytes);
		}
		return b;
	}

	public Writer createWriter() {
		return new Writer();
	}

	public Reader createReader() {
		return createReader(DEFAULT_BNODE_TRANSFORMER);
	}

	public Reader createReader(BiFunction<String,ValueFactory,Resource> bnodeTransformer) {
		return new Reader(bnodeTransformer);
	}


	@ThreadSafe
	public final class Writer {
		private Writer() {
		}

		private ByteBuffer writeTriple(Triple t, ByteBuffer buf) {
			buf = ByteUtils.ensureCapacity(buf, 1);
			buf.put(HeaderBytes.TRIPLE_TYPE);
			buf = writeValueWithSizeHeader(t.getSubject(), buf, Short.BYTES);
			buf = writeValueWithSizeHeader(t.getPredicate(), buf, Short.BYTES);
			buf = writeValueWithSizeHeader(t.getObject(), buf, Integer.BYTES);
			return buf;
		}

		public ByteBuffer writeValueWithSizeHeader(Value v, ByteBuffer buf, int sizeHeaderBytes) {
			buf = ByteUtils.ensureCapacity(buf, sizeHeaderBytes);
			int sizePos = buf.position();
			int startPos = buf.position() + sizeHeaderBytes;
			buf.position(startPos);
			if (v != null) {
				buf = writeTo(v, buf);
			}
			int endPos = buf.position();
			int len = endPos - startPos;
			buf.position(sizePos);
			if (sizeHeaderBytes == Short.BYTES) {
				buf.putShort((short) len);
			} else if (sizeHeaderBytes == Integer.BYTES) {
				buf.putInt(len);
			} else {
				throw new AssertionError();
			}
			buf.position(endPos);
			return buf;
		}

		public ByteBuffer writeValueWithSizeHeader(Value v, DataOutput out, int sizeHeaderBytes, ByteBuffer tmp) throws IOException {
			int len;
			if (v != null) {
				tmp.clear();
				tmp = writeTo(v, tmp);
				tmp.flip();
				len = tmp.remaining();
			} else {
				len = 0;
			}
			if (sizeHeaderBytes == Short.BYTES) {
				out.writeShort((short) len);
			} else if (sizeHeaderBytes == Integer.BYTES) {
				out.writeInt(len);
			} else {
				throw new AssertionError();
			}
			if (len > 0) {
				out.write(tmp.array(), tmp.arrayOffset(), len);
			}
			return tmp;
		}

		public byte[] toBytes(Value v) {
			ByteBuffer tmp = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
			tmp = writeTo(v, tmp);
			tmp.flip();
			byte[] b = new byte[tmp.remaining()];
			tmp.get(b);
			return b;
		}

		public ByteBuffer writeTo(Value v, ByteBuffer b) {
			if (v.isIRI()) {
				return writeIRI((IRI)v, b);
			} else if (v.isBNode()) {
				return writeBNode((BNode)v, b);
			} else if (v.isLiteral()) {
				return writeLiteral((Literal)v, b);
			} else if (v.isTriple()) {
				return writeTriple((Triple)v, b);
			} else {
				throw new AssertionError(String.format("Unexpected RDF value: %s (%s)", v, v.getClass().getName()));
			}
		}

		private ByteBuffer writeIRI(IRI iri, ByteBuffer b) {
			Integer irihash = config.getWellKnownIRIHash(iri);
			if (irihash != null) {
				b = ByteUtils.ensureCapacity(b, 1 + IRI_HASH_SIZE);
				b.put(HeaderBytes.IRI_HASH_TYPE);
				b.putInt(irihash);
				return b;
			} else {
				String ns = iri.getNamespace();
				String localName = iri.getLocalName();
				boolean endSlashEncodable = false;
				Short nshash = config.getWellKnownNamespaceHash(ns);
				if (nshash == null) {
					// is it end-slash encodable?
					String s = iri.stringValue();
					int iriLen = s.length();
					// has to be at least x//
					if (iriLen >= 3 && s.charAt(iriLen-1) == '/') {
						int sepPos = s.lastIndexOf('/', iriLen-2);
						if (sepPos > 0) {
							ns = s.substring(0, sepPos+1);
							localName = s.substring(sepPos+1, iriLen-1);
							nshash = config.getWellKnownNamespaceHash(ns);
							endSlashEncodable = true;
						}
					}
				}
				if (nshash != null) {
					IRIEncodingNamespace iriEncoder = !localName.isEmpty() ? config.getIRIEncodingNamespace(nshash) : null;
					if (iriEncoder != null) {
						b = ByteUtils.ensureCapacity(b, 1 + NAMESPACE_HASH_SIZE);
						final int failsafeMark = b.position();
						b.put(endSlashEncodable ? HeaderBytes.END_SLASH_ENCODED_IRI_TYPE : HeaderBytes.ENCODED_IRI_TYPE);
						b.putShort(nshash);
						try {
							return iriEncoder.writeBytes(localName, b);
						} catch (Exception e) {
							LOGGER.warn("Possibly invalid IRI {} for encoded namespace {} - falling back to generic IRI encoder ({})", iri, ns, e.getMessage());
							LOGGER.debug("{} for {} failed", IRIEncodingNamespace.class.getSimpleName(), ns, e);
							// if the dedicated namespace encoder fails then fallback to the generic encoder
							b.position(failsafeMark);
						}
					}
					ByteBuffer localBytes = writeUncompressedString(localName);
					b = ByteUtils.ensureCapacity(b, 1 + NAMESPACE_HASH_SIZE + localBytes.remaining());
					b.put(HeaderBytes.NAMESPACE_HASH_TYPE);
					b.putShort(nshash);
					b.put(localBytes);
					return b;
				} else {
					byte schemeType;
					int prefixLen;
					String s = iri.stringValue();
					if (s.startsWith("http")) {
						prefixLen = 4;
						if (s.startsWith("://", prefixLen)) {
							prefixLen += 3;
							if (s.startsWith("dx.doi.org/", prefixLen)) {
								schemeType = DOI_HTTP_SCHEME;
								prefixLen += 11;
							} else {
								schemeType = HTTP_SCHEME;
							}
						} else if (s.startsWith("s://", prefixLen)) {
							prefixLen += 4;
							if (s.startsWith("dx.doi.org/", prefixLen)) {
								schemeType = DOI_HTTPS_SCHEME;
								prefixLen += 11;
							} else {
								schemeType = HTTPS_SCHEME;
							}
						} else {
							schemeType = 0;
							prefixLen = 0;
						}
					} else {
						schemeType = 0;
						prefixLen = 0;
					}
					if (schemeType != 0) {
						ByteBuffer restBytes = writeUncompressedString(s.substring(prefixLen));
						b = ByteUtils.ensureCapacity(b, 2+restBytes.remaining());
						b.put(HeaderBytes.COMPRESSED_IRI_TYPE).put(schemeType);
						b.put(restBytes);
					} else {
						ByteBuffer iriBytes = writeUncompressedString(s);
						b = ByteUtils.ensureCapacity(b, 1+iriBytes.remaining());
						b.put(HeaderBytes.IRI_TYPE);
						b.put(iriBytes);
					}
					return b;
				}
			}
		}

		private ByteBuffer writeBNode(BNode n, ByteBuffer b) {
			ByteBuffer idBytes = writeUncompressedString(n.getID());
			b = ByteUtils.ensureCapacity(b, 1+idBytes.remaining());
			b.put(HeaderBytes.BNODE_TYPE);
			b.put(idBytes);
			return b;
		}

		private ByteBuffer writeLiteral(Literal l, ByteBuffer b) {
			ByteWriter writer = byteWriters.get(l.getCoreDatatype());
			if (writer != null) {
				final int failsafeMark = b.position();
				try {
					return writer.writeBytes(l, b);
				} catch (Exception e) {
					LOGGER.warn("Possibly invalid literal: {} ({})", l, e.toString());
					LOGGER.debug("{} for {} failed", ByteWriter.class.getSimpleName(), l.getDatatype(), e);
					// if the dedicated writer fails then fallback to the generic writer
					b.position(failsafeMark);
				}
			}
			b = ByteUtils.ensureCapacity(b, 1 + Short.BYTES);
			b.put(HeaderBytes.DATATYPE_LITERAL_TYPE);
			int sizePos = b.position();
			int startPos = b.position()+2;
			b.position(startPos);
			b = writeIRI(l.getDatatype(), b);
			int endPos = b.position();
			b.position(sizePos);
			int datatypeLen = endPos - startPos;
			assert datatypeLen <= Short.MAX_VALUE;
			b.putShort((short) datatypeLen);
			b.position(endPos);
			ByteBuffer labelBytes = writeUncompressedString(l.getLabel());
			b = ByteUtils.ensureCapacity(b, labelBytes.remaining());
			b.put(labelBytes);
			return b;
		}
	}


	@ThreadSafe
	public class Reader {
		private final BiFunction<String,ValueFactory,Resource> bnodeTransformer;

		private Reader(BiFunction<String,ValueFactory,Resource> bnodeTransformer) {
			this.bnodeTransformer = bnodeTransformer;
		}

		private Triple readTriple(ByteBuffer b, ValueFactory vf) {
			Resource s = (Resource) readValueWithSizeHeader(b, vf, Short.BYTES);
			IRI p = (IRI) readValueWithSizeHeader(b, vf, Short.BYTES);
			Value o = readValueWithSizeHeader(b, vf, Integer.BYTES);
			return vf.createTriple(s, p, o);
		}

		public Value readValueWithSizeHeader(ByteBuffer buf, ValueFactory vf, int sizeHeaderBytes) {
			int len;
			if (sizeHeaderBytes == Short.BYTES) {
				len = buf.getShort();
			} else if (sizeHeaderBytes == Integer.BYTES) {
				len = buf.getInt();
			} else {
				throw new AssertionError();
			}
			if (len == 0) {
				return null;
			}
			int originalLimit = buf.limit();
			buf.limit(buf.position() + len);
			Value v = readValue(buf, vf);
			buf.limit(originalLimit);
			return v;
		}

		public Value readValueWithSizeHeader(DataInput in, ValueFactory vf, int sizeHeaderBytes) throws IOException {
			int len;
			if (sizeHeaderBytes == Short.BYTES) {
				len = in.readShort();
			} else if (sizeHeaderBytes == Integer.BYTES) {
				len = in.readInt();
			} else {
				throw new AssertionError();
			}
			if (len == 0) {
				return null;
			}
			byte[] b = new byte[len];
			in.readFully(b);
			return readValue(ByteBuffer.wrap(b), vf);
		}

		public Value readValue(ByteBuffer b, ValueFactory vf) {
			int type = b.get();
			switch(type) {
				case HeaderBytes.IRI_TYPE:
					return vf.createIRI(readUncompressedString(b));
				case HeaderBytes.COMPRESSED_IRI_TYPE:
					{
						int schemeType = b.get();
						String prefix;
						switch (schemeType) {
							case HTTP_SCHEME:
								prefix = "http://";
								break;
							case HTTPS_SCHEME:
								prefix = "https://";
								break;
							case DOI_HTTP_SCHEME:
								prefix = "http://dx.doi.org/";
								break;
							case DOI_HTTPS_SCHEME:
								prefix = "https://dx.doi.org/";
								break;
							default:
								throw new AssertionError(String.format("Unexpected scheme type: %d", schemeType));
						}
						String s = readUncompressedString(b);
						return vf.createIRI(prefix + s);
					}
				case HeaderBytes.IRI_HASH_TYPE:
					{
						b.mark();
						int irihash = b.getInt(); // 32-bit hash
						IRI iri = config.getWellKnownIRI(irihash);
						if (iri == null) {
							b.limit(b.position()).reset();
							throw new IllegalStateException(String.format("Unknown IRI hash: %s (%d)", ByteUtils.encode(b), irihash));
						}
						return iri;
					}
				case HeaderBytes.NAMESPACE_HASH_TYPE:
					{
						b.mark();
						short nshash = b.getShort(); // 16-bit hash
						String namespace = config.getWellKnownNamespace(nshash);
						String localName = readUncompressedString(b);
						if (namespace == null) {
							b.limit(b.position()).reset();
							throw new IllegalStateException(String.format("Unknown namespace hash: %s (%d) (local name %s)", ByteUtils.encode(b), nshash, localName));
						}
						return vf.createIRI(namespace, localName);
					}
				case HeaderBytes.ENCODED_IRI_TYPE:
					{
						b.mark();
						short nshash = b.getShort(); // 16-bit hash
						IRIEncodingNamespace iriEncoder = config.getIRIEncodingNamespace(nshash);
						if (iriEncoder == null) {
							b.limit(b.position()).reset();
							throw new IllegalStateException(String.format("Unknown IRI encoder hash: %s (%d)", ByteUtils.encode(b), nshash));
						}
						return vf.createIRI(iriEncoder.getName(), iriEncoder.readBytes(b));
					}
				case HeaderBytes.END_SLASH_ENCODED_IRI_TYPE:
					{
						b.mark();
						short nshash = b.getShort(); // 16-bit hash
						IRIEncodingNamespace iriEncoder = config.getIRIEncodingNamespace(nshash);
						if (iriEncoder == null) {
							b.limit(b.position()).reset();
							throw new IllegalStateException(String.format("Unknown IRI encoder hash: %s (%d)", ByteUtils.encode(b), nshash));
						}
						return vf.createIRI(iriEncoder.getName()+iriEncoder.readBytes(b)+'/');
					}
				case HeaderBytes.BNODE_TYPE:
					return bnodeTransformer.apply(readUncompressedString(b), vf);
				case HeaderBytes.TRIPLE_TYPE:
					return readTriple(b, vf);
				case HeaderBytes.DATATYPE_LITERAL_TYPE:
					{
						int originalLimit = b.limit();
						int dtSize = b.getShort();
						b.limit(b.position()+dtSize);
						IRI datatype = (IRI) readValue(b, vf);
						b.limit(originalLimit);
						String label = readUncompressedString(b);
						return vf.createLiteral(label, datatype);
					}
				default:
					ByteReader reader = byteReaders.get(type);
					if (reader == null) {
						throw new AssertionError(String.format("Unexpected type: %s", type));
					}
					return reader.readBytes(b, vf);
			}
		}

		public ValueType getValueType(ByteBuffer b) {
			int type = b.get(b.position()); // peek
			switch(type) {
				case HeaderBytes.IRI_TYPE:
				case HeaderBytes.COMPRESSED_IRI_TYPE:
				case HeaderBytes.IRI_HASH_TYPE:
				case HeaderBytes.NAMESPACE_HASH_TYPE:
				case HeaderBytes.ENCODED_IRI_TYPE:
				case HeaderBytes.END_SLASH_ENCODED_IRI_TYPE:
					return ValueType.IRI;
				case HeaderBytes.BNODE_TYPE:
					if (bnodeTransformer == DEFAULT_BNODE_TRANSFORMER) {
						return ValueType.BNODE;
					} else {
						// unknown: BNode or skolem IRI
						return null;
					}
				case HeaderBytes.TRIPLE_TYPE:
					return ValueType.TRIPLE;
				case HeaderBytes.DATATYPE_LITERAL_TYPE:
					return ValueType.LITERAL;
				default:
					if (byteReaders.containsKey(type)) {
						return ValueType.LITERAL;
					} else {
						throw new AssertionError(String.format("Unexpected type: %s", type));
					}
			}
		}
	}
}
