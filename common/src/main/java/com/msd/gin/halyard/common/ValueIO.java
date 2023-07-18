package com.msd.gin.halyard.common;

import com.google.common.collect.Sets;
import com.ibm.icu.text.UnicodeCompressor;
import com.ibm.icu.text.UnicodeDecompressor;

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
import org.eclipse.rdf4j.model.datatypes.XMLDatatypeUtil;
import org.eclipse.rdf4j.model.vocabulary.GEO;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XSD;
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

	// compressed IRI types
	private static final byte HTTP_SCHEME = 'h';
	private static final byte HTTPS_SCHEME = 's';
	private static final byte DOI_HTTP_SCHEME = 'd';
	private static final byte DOI_HTTPS_SCHEME = 'D';

	private static ByteBuffer writeCalendar(byte type, XMLGregorianCalendar cal, ByteBuffer b) {
		BigDecimal fracSecs = cal.getFractionalSecond();
		long subMillis;
		if (fracSecs != null && fracSecs.scale() > 3) {
			BigDecimal fracMillis = fracSecs.scaleByPowerOfTen(3);
			subMillis = fracMillis.subtract(new BigDecimal(fracMillis.toBigInteger())).scaleByPowerOfTen(6).longValue();
		} else {
			subMillis = 0L;
		}
		b = ensureCapacity(b, (subMillis > 0) ? 1 + Long.BYTES + Short.BYTES + Long.BYTES : 1 + Long.BYTES + Short.BYTES);
		b.put(type).putLong(cal.toGregorianCalendar().getTimeInMillis());
		if (subMillis > 0) {
			b.putLong(subMillis);
		}
		if(cal.getTimezone() != DatatypeConstants.FIELD_UNDEFINED) {
			b.putShort((short) cal.getTimezone());
		}
		return b;
	}

	private static XMLGregorianCalendar readCalendar(ByteBuffer b) {
		long millis = b.getLong();
		long subMillis = (b.remaining() >= 4) ? b.getLong() : 0L;
		int tz = (b.remaining() >= 2) ? b.getShort() : Short.MIN_VALUE;
		GregorianCalendar c = newGregorianCalendar();
		if (tz != Short.MIN_VALUE) {
			int tzHr = tz/60;
			int tzMin = tz - 60 * tzHr;
			c.setTimeZone(TimeZone.getTimeZone(String.format("GMT%+02d%02d", tzHr, tzMin)));
		}
		c.setTimeInMillis(millis);
		XMLGregorianCalendar cal = DATATYPE_FACTORY.newXMLGregorianCalendar(c);
		if (subMillis > 0) {
			BigDecimal fracSecs = cal.getFractionalSecond().add(BigDecimal.valueOf(subMillis, 9)).stripTrailingZeros();
			cal.setFractionalSecond(fracSecs);
		} else if (BigDecimal.ZERO.compareTo(cal.getFractionalSecond()) == 0) {
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
			case UNCOMPRESSED_STRING_TYPE:
				return readUncompressedString(b);
			case COMPRESSED_STRING_TYPE:
				return readCompressedString(b);
			case SCSU_STRING_TYPE:
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
		b = ensureCapacity(b, Integer.BYTES + compressed.remaining());
		return b.putInt(uncompressedLen).put(compressed);
	}

	private static String readCompressedString(ByteBuffer b) {
		ByteBuffer uncompressed = decompress(b);
		return readUncompressedString(uncompressed);
	}

	private static ByteBuffer compress(ByteBuffer b) {
		LZ4Compressor compressor = LZ4.highCompressor();
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
			b = ensureCapacity(b, 512);
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

	private static final int MAX_LONG_STRING_LENGTH = Long.toString(Long.MAX_VALUE).length();

	public static ByteBuffer writeCompressedInteger(String s, ByteBuffer b) {
		BigInteger bigInt;
		long x;
		if (s.length() < MAX_LONG_STRING_LENGTH) {
			x = XMLDatatypeUtil.parseLong(s);
			bigInt = null;
		} else {
			bigInt = XMLDatatypeUtil.parseInteger(s);
			double u = bigInt.doubleValue();
			if (u >= Long.MIN_VALUE && u <= Long.MAX_VALUE) {
				x = bigInt.longValueExact();
				bigInt = null;
			} else {
				x = 0;
			}
		}
		return writeCompressedInteger(bigInt, x, b);
	}

	private static ByteBuffer writeCompressedInteger(BigInteger bigInt, long x, ByteBuffer b) {
		if (bigInt != null) {
			byte[] bytes = bigInt.toByteArray();
			b = ensureCapacity(b, 1 + bytes.length);
			return b.put(BIG_INT_TYPE).put(bytes);
		} else if (x >= Short.MIN_VALUE && x <= Short.MAX_VALUE) {
			b = ensureCapacity(b, 1 + Short.BYTES);
			return b.put(SHORT_COMPRESSED_BIG_INT_TYPE).putShort((short) x);
		} else if (x >= Integer.MIN_VALUE && x <= Integer.MAX_VALUE) {
			b = ensureCapacity(b, 1 + Integer.BYTES);
			return b.put(INT_COMPRESSED_BIG_INT_TYPE).putInt((int) x);
		} else {
			b = ensureCapacity(b, 1 + Long.BYTES);
			return b.put(LONG_COMPRESSED_BIG_INT_TYPE).putLong(x);
		}
	}

	public static String readCompressedInteger(ByteBuffer b) {
		int type = b.get();
		switch (type) {
			case SHORT_COMPRESSED_BIG_INT_TYPE:
				return Short.toString(b.getShort());
			case INT_COMPRESSED_BIG_INT_TYPE:
				return Integer.toString(b.getInt());
			case LONG_COMPRESSED_BIG_INT_TYPE:
				return Long.toString(b.getLong());
			case BIG_INT_TYPE:
				byte[] bytes = new byte[b.remaining()];
				b.get(bytes);
				return new BigInteger(bytes).toString();
			default:
				throw new AssertionError(String.format("Unrecognized compressed integer type: %d", type));
		}
	}

	public static ByteBuffer ensureCapacity(ByteBuffer b, int requiredSize) {
		if (b.remaining() < requiredSize) {
			// leave some spare capacity
			ByteBuffer newb = ByteBuffer.allocate(3*b.capacity()/2 + 2*requiredSize);
			b.flip();
			newb.put(b);
			return newb;
		} else {
			return b;
		}
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
	private final Map<IRI, ByteWriter> byteWriters = new HashMap<>(32);
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

	private void addByteWriter(IRI datatype, ByteWriter bw) {
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
		addByteWriter(XSD.BOOLEAN, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				boolean v = l.booleanValue();
				b = ensureCapacity(b, 1);
				return b.put(v ? TRUE_TYPE : FALSE_TYPE);
			}
		});
		addByteReader(FALSE_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				return vf.createLiteral(false);
			}
		});
		addByteReader(TRUE_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				return vf.createLiteral(true);
			}
		});

		addByteWriter(XSD.BYTE, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				byte v = l.byteValue();
				return ensureCapacity(b, 1 + Byte.BYTES).put(BYTE_TYPE).put(v);
			}
		});
		addByteReader(BYTE_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				return vf.createLiteral(b.get());
			}
		});

		addByteWriter(XSD.SHORT, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				short v = l.shortValue();
				return ensureCapacity(b, 1 + Short.BYTES).put(SHORT_TYPE).putShort(v);
			}
		});
		addByteReader(SHORT_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				return vf.createLiteral(b.getShort());
			}
		});

		addByteWriter(XSD.INT, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				int v = l.intValue();
				return ensureCapacity(b, 1 + Integer.BYTES).put(INT_TYPE).putInt(v);
			}
		});
		addByteReader(INT_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				return vf.createLiteral(b.getInt());
			}
		});

		addByteWriter(XSD.LONG, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				long v = l.longValue();
				return ensureCapacity(b, 1 + Long.BYTES).put(LONG_TYPE).putLong(v);
			}
		});
		addByteReader(LONG_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				return vf.createLiteral(b.getLong());
			}
		});

		addByteWriter(XSD.FLOAT, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				float v = l.floatValue();
				return ensureCapacity(b, 1 + Float.BYTES).put(FLOAT_TYPE).putFloat(v);
			}
		});
		addByteReader(FLOAT_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				return vf.createLiteral(b.getFloat());
			}
		});

		addByteWriter(XSD.DOUBLE, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				double v = l.doubleValue();
				return ensureCapacity(b, 1 + Double.BYTES).put(DOUBLE_TYPE).putDouble(v);
			}
		});
		addByteReader(DOUBLE_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				return vf.createLiteral(b.getDouble());
			}
		});

		addByteWriter(XSD.INTEGER, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				BigInteger bigInt;
				long x;
				if (l.getLabel().length() < MAX_LONG_STRING_LENGTH) {
					x = l.longValue();
					bigInt = null;
				} else {
					bigInt = l.integerValue();
					double u = bigInt.doubleValue();
					if (u >= Long.MIN_VALUE && u <= Long.MAX_VALUE) {
						x = bigInt.longValueExact();
						bigInt = null;
					} else {
						x = 0;
					}
				}
				return writeCompressedInteger(bigInt, x, b);
			}
		});
		addByteReader(BIG_INT_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				byte[] bytes = new byte[b.remaining()];
				b.get(bytes);
				return vf.createLiteral(new BigInteger(bytes));
			}
		});
		addByteReader(INT_COMPRESSED_BIG_INT_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				return new IntLiteral(b.getInt(), CoreDatatype.XSD.INTEGER);
			}
		});
		addByteReader(SHORT_COMPRESSED_BIG_INT_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				return new IntLiteral(b.getShort(), CoreDatatype.XSD.INTEGER);
			}
		});

		addByteWriter(XSD.DECIMAL, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				BigDecimal x = l.decimalValue();
				byte[] bytes = x.unscaledValue().toByteArray();
				int scale = x.scale();
				b = ensureCapacity(b, 1 + Integer.BYTES + bytes.length);
				return b.put(BIG_FLOAT_TYPE).putInt(scale).put(bytes);
			}
		});
		addByteReader(BIG_FLOAT_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				int scale = b.getInt();
				byte[] bytes = new byte[b.remaining()];
				b.get(bytes);
				return vf.createLiteral(new BigDecimal(new BigInteger(bytes), scale));
			}
		});

		addByteWriter(XSD.STRING, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				return writeString(l.getLabel(), b);
			}
		});
		addByteReader(COMPRESSED_STRING_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				return vf.createLiteral(readCompressedString(b));
			}
		});
		addByteReader(UNCOMPRESSED_STRING_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				return vf.createLiteral(readUncompressedString(b));
			}
		});

		addByteWriter(RDF.LANGSTRING, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				String langTag = l.getLanguage().get();
				b = writeLanguagePrefix(langTag, b);
				return writeString(l.getLabel(), b, langTag);
			}
		});
		addByteReader(LANGUAGE_HASH_LITERAL_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				b.mark();
				short langHash = b.getShort(); // 16-bit hash
				String label = readString(b);
				String lang = config.getLanguageTag(langHash);
				if (lang == null) {
					b.limit(b.position()).reset();
					throw new IllegalStateException(String.format("Unknown language tag hash: %s (%d) (label %s)", Hashes.encode(b), langHash, label));
				}
				return vf.createLiteral(label, lang);
			}
		});
		addByteReader(LANGUAGE_LITERAL_TYPE, new ByteReader() {
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

		addByteWriter(XSD.TIME, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				return writeCalendar(TIME_TYPE, l.calendarValue(), b);
			}
		});
		addByteReader(TIME_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				XMLGregorianCalendar cal = readCalendar(b);
				cal.setYear(null);
				cal.setMonth(DatatypeConstants.FIELD_UNDEFINED);
				cal.setDay(DatatypeConstants.FIELD_UNDEFINED);
				return vf.createLiteral(cal);
			}
		});

		addByteWriter(XSD.DATE, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				return writeCalendar(DATE_TYPE, l.calendarValue(), b);
			}
		});
		addByteReader(DATE_TYPE, new ByteReader() {
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

		addByteWriter(XSD.DATETIME, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				return writeCalendar(DATETIME_TYPE, l.calendarValue(), b);
			}
		});
		addByteReader(DATETIME_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				XMLGregorianCalendar cal = readCalendar(b);
				return vf.createLiteral(cal);
			}
		});

		addByteWriter(GEO.WKT_LITERAL, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				try {
					byte[] wkb = WKTLiteral.writeWKB(l);
					b = ensureCapacity(b, 2 + wkb.length);
					b.put(WKT_LITERAL_TYPE);
					b.put(WKT_LITERAL_TYPE); // mark as valid
					return b.put(wkb);
				} catch (ParseException e) {
					ByteBuffer wktBytes = writeUncompressedString(l.getLabel());
					b = ensureCapacity(b, 2 + wktBytes.remaining());
					b.put(WKT_LITERAL_TYPE);
					b.put(UNCOMPRESSED_STRING_TYPE); // mark as invalid
					return b.put(wktBytes);
				}
			}
		});
		addByteReader(WKT_LITERAL_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				int wktType = b.get();
				switch (wktType) {
					case WKT_LITERAL_TYPE:
						// valid wkt
						byte[] wkbBytes = new byte[b.remaining()];
						b.get(wkbBytes);
						return new WKTLiteral(wkbBytes);
					case UNCOMPRESSED_STRING_TYPE:
						// invalid xml
						return vf.createLiteral(readUncompressedString(b), GEO.WKT_LITERAL);
					default:
						throw new AssertionError(String.format("Unrecognized WKT type: %d", wktType));
				}
			}
		});

		addByteWriter(RDF.XMLLITERAL, new ByteWriter() {
			@Override
			public ByteBuffer writeBytes(Literal l, ByteBuffer b) {
				try {
					ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
					XMLLiteral.writeInfoset(l.getLabel(), out);
					byte[] xb = out.toByteArray();
					b = ensureCapacity(b, 2 + xb.length);
					b.put(XML_TYPE);
					b.put(XML_TYPE); // mark xml as valid
					return b.put(xb);
				} catch(TransformerException e) {
					b = ensureCapacity(b, 2);
					b.put(XML_TYPE);
					b.put(COMPRESSED_STRING_TYPE); // mark xml as invalid
					return writeCompressedString(l.getLabel(), b);
				}
			}
		});
		addByteReader(XML_TYPE, new ByteReader() {
			@Override
			public Literal readBytes(ByteBuffer b, ValueFactory vf) {
				int xmlType = b.get();
				switch (xmlType) {
					case XML_TYPE:
						// valid xml
						byte[] fiBytes = new byte[b.remaining()];
						b.get(fiBytes);
						return new XMLLiteral(fiBytes);
					case COMPRESSED_STRING_TYPE:
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
			b.put(SCSU_STRING_TYPE);
			return writeScsuString(s, b);
		} else {
			b.put(COMPRESSED_STRING_TYPE);
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
			b = ensureCapacity(b, 1 + Integer.BYTES + compressedLen);
			return b.put(COMPRESSED_STRING_TYPE).putInt(uncompressedLen).put(compressed);
		} else {
			if (compressed != null && LOGGER.isDebugEnabled()) {
				LOGGER.debug("Ineffective compression of string of byte length %d", uncompressedLen);
			}
			b = ensureCapacity(b, 1 + uncompressedLen);
			return b.put(UNCOMPRESSED_STRING_TYPE).put(uncompressed);
		}
	}

	private ByteBuffer writeLanguagePrefix(String langTag, ByteBuffer b) {
		Short hash = config.getLanguageTagHash(langTag);
		if (hash != null) {
			b = ensureCapacity(b, 1+LANG_HASH_SIZE);
			b.put(LANGUAGE_HASH_LITERAL_TYPE);
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
			b = ensureCapacity(b, 1+1+langBytes.remaining());
			b.put(LANGUAGE_LITERAL_TYPE);
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
		return createReader((id,valueFactory) -> valueFactory.createBNode(id));
	}

	public Reader createReader(BiFunction<String,ValueFactory,Resource> bnodeTransformer) {
		return new Reader(bnodeTransformer);
	}


	@ThreadSafe
	public final class Writer {
		private Writer() {
		}

		private ByteBuffer writeTriple(Triple t, ByteBuffer buf) {
			buf = ensureCapacity(buf, 1);
			buf.put(TRIPLE_TYPE);
			buf = writeValueWithSizeHeader(t.getSubject(), buf, Short.BYTES);
			buf = writeValueWithSizeHeader(t.getPredicate(), buf, Short.BYTES);
			buf = writeValueWithSizeHeader(t.getObject(), buf, Integer.BYTES);
			return buf;
		}

		public ByteBuffer writeValueWithSizeHeader(Value v, ByteBuffer buf, int sizeHeaderBytes) {
			buf = ValueIO.ensureCapacity(buf, sizeHeaderBytes);
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
			Integer irihash = config.getIRIHash(iri);
			if (irihash != null) {
				b = ensureCapacity(b, 1 + IRI_HASH_SIZE);
				b.put(IRI_HASH_TYPE);
				b.putInt(irihash);
				return b;
			} else {
				String ns = iri.getNamespace();
				String localName = iri.getLocalName();
				boolean endSlashEncodable = false;
				Short nshash = config.getNamespaceHash(ns);
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
							nshash = config.getNamespaceHash(ns);
							endSlashEncodable = true;
						}
					}
				}
				if (nshash != null) {
					IRIEncodingNamespace iriEncoder = !localName.isEmpty() ? config.getIRIEncodingNamespace(nshash) : null;
					if (iriEncoder != null) {
						b = ensureCapacity(b, 1 + NAMESPACE_HASH_SIZE);
						final int failsafeMark = b.position();
						b.put(endSlashEncodable ? END_SLASH_ENCODED_IRI_TYPE : ENCODED_IRI_TYPE);
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
					b = ensureCapacity(b, 1 + NAMESPACE_HASH_SIZE + localBytes.remaining());
					b.put(NAMESPACE_HASH_TYPE);
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
						b = ensureCapacity(b, 2+restBytes.remaining());
						b.put(COMPRESSED_IRI_TYPE).put(schemeType);
						b.put(restBytes);
					} else {
						ByteBuffer iriBytes = writeUncompressedString(s);
						b = ensureCapacity(b, 1+iriBytes.remaining());
						b.put(IRI_TYPE);
						b.put(iriBytes);
					}
					return b;
				}
			}
		}

		private ByteBuffer writeBNode(BNode n, ByteBuffer b) {
			ByteBuffer idBytes = writeUncompressedString(n.getID());
			b = ensureCapacity(b, 1+idBytes.remaining());
			b.put(BNODE_TYPE);
			b.put(idBytes);
			return b;
		}

		private ByteBuffer writeLiteral(Literal l, ByteBuffer b) {
			ByteWriter writer = byteWriters.get(l.getDatatype());
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
			b = ensureCapacity(b, 1 + Short.BYTES);
			b.put(DATATYPE_LITERAL_TYPE);
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
			b = ensureCapacity(b, labelBytes.remaining());
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
				case IRI_TYPE:
					return vf.createIRI(readUncompressedString(b));
				case COMPRESSED_IRI_TYPE:
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
				case IRI_HASH_TYPE:
					{
						b.mark();
						int irihash = b.getInt(); // 32-bit hash
						IRI iri = config.getIRI(irihash);
						if (iri == null) {
							b.limit(b.position()).reset();
							throw new IllegalStateException(String.format("Unknown IRI hash: %s (%d)", Hashes.encode(b), irihash));
						}
						return iri;
					}
				case NAMESPACE_HASH_TYPE:
					{
						b.mark();
						short nshash = b.getShort(); // 16-bit hash
						String namespace = config.getNamespace(nshash);
						String localName = readUncompressedString(b);
						if (namespace == null) {
							b.limit(b.position()).reset();
							throw new IllegalStateException(String.format("Unknown namespace hash: %s (%d) (local name %s)", Hashes.encode(b), nshash, localName));
						}
						return vf.createIRI(namespace, localName);
					}
				case ENCODED_IRI_TYPE:
					{
						b.mark();
						short nshash = b.getShort(); // 16-bit hash
						IRIEncodingNamespace iriEncoder = config.getIRIEncodingNamespace(nshash);
						if (iriEncoder == null) {
							b.limit(b.position()).reset();
							throw new IllegalStateException(String.format("Unknown IRI encoder hash: %s (%d)", Hashes.encode(b), nshash));
						}
						return vf.createIRI(iriEncoder.getName(), iriEncoder.readBytes(b));
					}
				case END_SLASH_ENCODED_IRI_TYPE:
					{
						b.mark();
						short nshash = b.getShort(); // 16-bit hash
						IRIEncodingNamespace iriEncoder = config.getIRIEncodingNamespace(nshash);
						if (iriEncoder == null) {
							b.limit(b.position()).reset();
							throw new IllegalStateException(String.format("Unknown IRI encoder hash: %s (%d)", Hashes.encode(b), nshash));
						}
						return vf.createIRI(iriEncoder.getName()+iriEncoder.readBytes(b)+'/');
					}
				case BNODE_TYPE:
					return bnodeTransformer.apply(readUncompressedString(b), vf);
				case DATATYPE_LITERAL_TYPE:
					{
						int originalLimit = b.limit();
						int dtSize = b.getShort();
						b.limit(b.position()+dtSize);
						IRI datatype = (IRI) readValue(b, vf);
						b.limit(originalLimit);
						String label = readUncompressedString(b);
						return vf.createLiteral(label, datatype);
					}
				case TRIPLE_TYPE:
					return readTriple(b, vf);
				default:
					ByteReader reader = byteReaders.get(type);
					if (reader == null) {
						throw new AssertionError(String.format("Unexpected type: %s", type));
					}
					return reader.readBytes(b, vf);
			}
		}
	}
}
