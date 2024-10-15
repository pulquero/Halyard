package com.msd.gin.halyard.query.algebra.evaluation.function;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.math.BigInteger;
import java.net.URI;
import java.time.Duration;
import java.time.Period;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.xml.namespace.QName;
import javax.xml.xpath.XPathFunction;
import javax.xml.xpath.XPathFunctionException;
import javax.xml.xpath.XPathFunctionResolver;

import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.model.vocabulary.SP;
import org.eclipse.rdf4j.model.vocabulary.SPIN;
import org.eclipse.rdf4j.model.vocabulary.SPL;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.eclipse.rdf4j.query.algebra.evaluation.function.FunctionRegistry;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;

import com.msd.gin.halyard.model.MapLiteral;
import com.msd.gin.halyard.model.ObjectArrayLiteral;
import com.msd.gin.halyard.model.ObjectLiteral;
import com.msd.gin.halyard.model.vocabulary.HALYARD;

import net.sf.saxon.Configuration;
import net.sf.saxon.expr.Expression;
import net.sf.saxon.expr.JPConverter;
import net.sf.saxon.expr.PJConverter;
import net.sf.saxon.expr.StaticContext;
import net.sf.saxon.expr.StaticProperty;
import net.sf.saxon.expr.XPathContext;
import net.sf.saxon.functions.FunctionLibrary;
import net.sf.saxon.functions.FunctionLibraryList;
import net.sf.saxon.functions.registry.BuiltInFunctionSet;
import net.sf.saxon.functions.registry.ConstructorFunctionLibrary;
import net.sf.saxon.ma.arrays.ArrayItem;
import net.sf.saxon.ma.arrays.SimpleArrayItem;
import net.sf.saxon.ma.map.DictionaryMap;
import net.sf.saxon.ma.map.KeyValuePair;
import net.sf.saxon.ma.map.MapItem;
import net.sf.saxon.om.GroundedValue;
import net.sf.saxon.om.StructuredQName;
import net.sf.saxon.str.StringView;
import net.sf.saxon.sxpath.IndependentContext;
import net.sf.saxon.trans.SymbolicName;
import net.sf.saxon.trans.XPathException;
import net.sf.saxon.type.AtomicType;
import net.sf.saxon.type.ItemType;
import net.sf.saxon.value.AtomicValue;
import net.sf.saxon.value.BigIntegerValue;
import net.sf.saxon.value.DayTimeDurationValue;
import net.sf.saxon.value.DurationValue;
import net.sf.saxon.value.SequenceExtent;
import net.sf.saxon.value.YearMonthDurationValue;

public class DynamicFunctionRegistry extends FunctionRegistry {
	private static final DynamicFunctionRegistry INSTANCE = new DynamicFunctionRegistry();

	public static DynamicFunctionRegistry getInstance() {
		return INSTANCE;
	}

	private final FunctionResolver resolver;

	public DynamicFunctionRegistry() {
		this(new XPathSparqlFunctionResolver(new SaxonXPathFunctionResolver()));
	}

	public DynamicFunctionRegistry(FunctionResolver resolver) {
		this.resolver = resolver;
	}

	@Override
	public boolean has(String iri) {
		return get(iri).isPresent();
	}

	@Override
	public Optional<Function> get(String iri) {
		return Optional.ofNullable(services.computeIfAbsent(iri, resolver::resolveFunction));
	}


	public static final class XPathSparqlFunctionResolver implements FunctionResolver {
		private final XPathFunctionResolver resolver;

		XPathSparqlFunctionResolver(XPathFunctionResolver resolver) {
			this.resolver = resolver;
		}

		@Override
		public Function resolveFunction(String iri) {
			int sep = iri.lastIndexOf('#');
			if (sep != -1) {
				QName qname = new QName(iri.substring(0, sep), iri.substring(sep + 1));
				Map<Integer, XPathFunction> arityMap = null;
				for (int i = 0; i < 20; i++) {
					XPathFunction xpathFunc = resolver.resolveFunction(qname, i);
					if (xpathFunc != null) {
						if (arityMap == null) {
							arityMap = new HashMap<>(7);
						}
						arityMap.put(i, xpathFunc);
					}
				}

				if (arityMap != null && !arityMap.isEmpty()) {
					return new XPathSparqlFunction(iri, arityMap);
				}
			}
			return null;
		}
	}


	static final class XPathSparqlFunction implements Function {
		private static final Map<IRI, java.util.function.Function<Literal, Object>> LITERAL_CONVERTERS = new HashMap<>(63);
		private static final java.util.function.Function<Literal, Object> DEFAULT_CONVERTER = l -> {
			if (l instanceof ObjectLiteral) {
				return ((ObjectLiteral<?>)l).objectValue();
			} else {
				IRI dt = l.getDatatype();
				if (HALYARD.ARRAY_TYPE.equals(dt)) {
					return ObjectArrayLiteral.objectArray(l);
				} else if (HALYARD.MAP_TYPE.equals(dt)) {
					return MapLiteral.objectMap(l);
				} else {
					return l.getLabel();
				}
			}
		};
		private final String name;
		private final Map<Integer, XPathFunction> arityMap;

		static {
			LITERAL_CONVERTERS.put(XSD.STRING, Literal::getLabel);
			LITERAL_CONVERTERS.put(XSD.FLOAT, Literal::floatValue);
			LITERAL_CONVERTERS.put(XSD.DOUBLE, Literal::doubleValue);
			LITERAL_CONVERTERS.put(XSD.DECIMAL, Literal::decimalValue);
			LITERAL_CONVERTERS.put(XSD.INTEGER, l -> {
				BigInteger v = l.integerValue();
				if (v.compareTo(BigIntegerValue.MIN_LONG) < 0 || v.compareTo(BigIntegerValue.MAX_LONG) > 0) {
					return v;
				} else {
					long lv = v.longValue();
					if (lv < Integer.MIN_VALUE || lv > Integer.MAX_VALUE) {
						return lv;
					} else {
						return (int) lv;
					}
				}
			});
			LITERAL_CONVERTERS.put(XSD.INT, Literal::intValue);
			LITERAL_CONVERTERS.put(XSD.SHORT, Literal::shortValue);
			LITERAL_CONVERTERS.put(XSD.BYTE, Literal::byteValue);
			LITERAL_CONVERTERS.put(XSD.DATETIME, l -> l.calendarValue().toGregorianCalendar().getTime());
			LITERAL_CONVERTERS.put(XSD.BOOLEAN, Literal::booleanValue);
			LITERAL_CONVERTERS.put(XSD.DAYTIMEDURATION, l -> Duration.parse(l.getLabel()));
			LITERAL_CONVERTERS.put(XSD.YEARMONTHDURATION, l -> Period.parse(l.getLabel()));
			LITERAL_CONVERTERS.put(XSD.DURATION, l -> DurationValue.makeDuration(StringView.of(l.getLabel())));
		}

		XPathSparqlFunction(String name, Map<Integer, XPathFunction> arityMap) {
			this.name = name;
			this.arityMap = arityMap;
		}

		@Override
		public String getURI() {
			return name;
		}

		@Override
		public Value evaluate(ValueFactory vf, Value... args) throws ValueExprEvaluationException {
			XPathFunction f = arityMap.get(args.length);
			if (f == null) {
				throw new ValueExprEvaluationException("Incorrect number of arguments");
			}

			List<Object> xargs = new ArrayList<>(args.length);
			for (Value arg : args) {
				// convert from Value to Java object
				Object xarg;
				if (arg instanceof Literal) {
					Literal l = (Literal) arg;
					IRI dt = l.getDatatype();
					xarg = LITERAL_CONVERTERS.getOrDefault(dt, DEFAULT_CONVERTER).apply(l);
				} else if (arg instanceof IRI) {
					xarg = URI.create(arg.stringValue());
				} else {
					xarg = arg.stringValue();
				}
				xargs.add(xarg);
			}
			try {
				return toValue(vf, f.evaluate(xargs));
			} catch (XPathFunctionException | XPathException ex) {
				throw new ValueExprEvaluationException(ex);
			}
		}
	}

	private static Value toValue(ValueFactory vf, Object v) throws XPathException {
		if (v instanceof Object[]) {
			return new ObjectArrayLiteral((Object[]) v);
		} else if (v instanceof Map<?,?>) {
			return new MapLiteral((Map<String,Object>)v);
		} else {
			return org.eclipse.rdf4j.model.util.Values.literal(vf, v, true);
		}
	}


	static final class SaxonXPathFunctionResolver implements XPathFunctionResolver {
		private final FunctionLibraryList lib;
		private final StaticContext ctx;

		SaxonXPathFunctionResolver() {
			Configuration config = new Configuration();
			FunctionLibraryList lib = new FunctionLibraryList();
			lib.addFunctionLibrary(config.getXPath31FunctionSet());
			lib.addFunctionLibrary(config.getBuiltInExtensionLibraryList());
			lib.addFunctionLibrary(new ConstructorFunctionLibrary(config));
			lib.addFunctionLibrary(config.getIntegratedFunctionLibrary());
			this.lib = lib;
			this.ctx = new IndependentContext(config);
		}

		@Override
		public XPathFunction resolveFunction(QName functionName, int arity) {
			SymbolicName.F name = new SymbolicName.F(new StructuredQName("", functionName.getNamespaceURI(), functionName.getLocalPart()), arity);
			return lib.isAvailable(name, 31) ? new SaxonXPathFunction(name, lib, ctx) : null;
		}
	}


	static final class SaxonXPathFunction implements XPathFunction {
		private final SymbolicName.F name;
		private final FunctionLibrary lib;
		private final StaticContext ctx;
		private final Map<Class<?>,JPConverter> jpConverters = new HashMap<>();
		private final Map<AtomicType,PJConverter> pjConverters = new HashMap<>();

		SaxonXPathFunction(SymbolicName.F name, FunctionLibrary lib, StaticContext ctx) {
			this.name = name;
			this.lib = lib;
			this.ctx = ctx;
		}

		@Override
		public Object evaluate(@SuppressWarnings("rawtypes") List args) throws XPathFunctionException {
			XPathContext xctx = ctx.makeEarlyEvaluationContext();
			Expression[] staticArgs = new Expression[args.size()];
			for (int i = 0; i < args.size(); i++) {
				Object arg = args.get(i);
				GroundedValue v;
				try {
					v = toGroundedValue(arg, xctx);
				} catch (XPathException ex) {
					throw new XPathFunctionException(ex);
				}
				staticArgs[i] = net.sf.saxon.expr.Literal.makeLiteral(v);
			}
			List<String> reasons = new ArrayList<>(0);
			Expression expr = lib.bind(name, staticArgs, ctx, reasons);
			if (expr == null) {
				throw new XPathFunctionException(String.format("No such function %s: %s", name, reasons));
			}
			try {
				GroundedValue result = SequenceExtent.from(expr.iterate(xctx)).reduce();
				PJConverter converter = PJConverter.allocate(ctx.getConfiguration(), expr.getItemType(), expr.getCardinality(), Object.class);
				return toObject(converter.convert(result, Object.class, xctx), xctx);
			} catch (XPathException ex) {
				throw new XPathFunctionException(ex);
			}
		}

		/**
		 * Convert from Java object to Saxon value.
		 */
		private GroundedValue toGroundedValue(Object o, XPathContext xctx) throws XPathException {
			GroundedValue v;
			if (o instanceof Period) {
				v = YearMonthDurationValue.fromMonths((int) ((Period) o).toTotalMonths());
			} else if (o instanceof Duration) {
				v = DayTimeDurationValue.fromJavaDuration((Duration) o);
			} else if (o instanceof Object[]) {
				Object[] arr = (Object[]) o;
				List<GroundedValue> gvs = new ArrayList<>(arr.length);
				for (Object e : arr) {
					gvs.add(toGroundedValue(e, xctx));
				}
				v = new SimpleArrayItem(gvs);
			} else if (o instanceof Map<?,?>) {
				Map<String,Object> map = (Map<String,Object>) o;
				DictionaryMap mv = new DictionaryMap();
				for (Map.Entry<String,Object> entry : map.entrySet()) {
					mv.initialPut(entry.getKey(), toGroundedValue(entry.getValue(), xctx));
				}
				v = mv;
			} else {
				JPConverter converter = jpConverters.computeIfAbsent(o.getClass(), cls -> JPConverter.allocate(cls, null, ctx.getConfiguration()));
				v = converter.convert(o, xctx).materialize();
			}
			return v;
		}

		private Object toObject(Object v, XPathContext xctx) throws XPathException {
			if (v instanceof ArrayItem) {
				ArrayItem ai = (ArrayItem) v;
				Object[] arr = new Object[ai.arrayLength()];
				for (int i=0; i<arr.length; i++) {
					arr[i] = fromGroundedValue(ai.get(i), xctx);
				}
				return arr;
			} else if (v instanceof MapItem) {
				MapItem mi = (MapItem) v;
				Map<String,Object> map = new HashMap<>(mi.size()+1);
				for (KeyValuePair kv : mi.keyValuePairs()) {
					map.put((String) fromGroundedValue(kv.key, xctx), fromGroundedValue(kv.value, xctx));
				}
				return map;
			} else {
				return v;
			}
		}

		private Object fromGroundedValue(GroundedValue v, XPathContext xctx) throws XPathException {
			if (!(v instanceof AtomicValue)) {
				throw new XPathException("Unsupported type: "+v.getClass());
			}
			PJConverter converter = pjConverters.computeIfAbsent(((AtomicValue)v).getItemType(),
					t -> {
						try {
							return PJConverter.allocate(ctx.getConfiguration(), t, StaticProperty.EXACTLY_ONE, Object.class);
						} catch (XPathException e) {
							throw new RuntimeException(e);
						}
					});
			return converter.convert(v, Object.class, xctx);
		}
	}

	public static void main(String[] args) throws Exception {
		Field funcTableField = BuiltInFunctionSet.class.getDeclaredField("functionTable");
		funcTableField.setAccessible(true);
		SimpleValueFactory vf = SimpleValueFactory.getInstance();
		SaxonXPathFunctionResolver saxon = new SaxonXPathFunctionResolver();
		try (OutputStream out = new FileOutputStream("xsl-functions.ttl")) {
			RDFWriter writer = Rio.createWriter(RDFFormat.TURTLE, out);
			writer.startRDF();
			writer.handleNamespace(RDF.PREFIX, RDF.NAMESPACE);
			writer.handleNamespace(RDFS.PREFIX, RDFS.NAMESPACE);
			writer.handleNamespace(XSD.PREFIX, XSD.NAMESPACE);
			writer.handleNamespace(SPIN.PREFIX, SPIN.NAMESPACE);
			writer.handleNamespace(SP.PREFIX, SP.NAMESPACE);
			writer.handleNamespace(SPL.PREFIX, SPL.NAMESPACE);
			IRI xpathFunctionsClass = vf.createIRI(SPL.NAMESPACE, "XPathFunctions");
			for (FunctionLibrary lib : saxon.lib.getLibraryList()) {
				if (lib instanceof BuiltInFunctionSet) {
					BuiltInFunctionSet funcs = (BuiltInFunctionSet) lib;
					Map<String,BuiltInFunctionSet.Entry> funcTable = (Map<String,BuiltInFunctionSet.Entry>) funcTableField.get(funcs);
					for (BuiltInFunctionSet.Entry funcInfo : funcTable.values()) {
						IRI funcIri = toIRI(funcInfo.name, vf);
						writer.handleStatement(vf.createStatement(funcIri, RDF.TYPE, SPIN.FUNCTION_CLASS));
						writer.handleStatement(vf.createStatement(funcIri, RDFS.LABEL, vf.createLiteral(funcInfo.name.getDisplayName())));
						writer.handleStatement(vf.createStatement(funcIri, RDFS.SUBCLASSOF, xpathFunctionsClass));
						if (funcInfo.itemType.isAtomicType()) {
							writer.handleStatement(vf.createStatement(funcIri, SPIN.RETURN_TYPE_PROPERTY, toIRI(funcInfo.itemType.getAtomizedItemType().getTypeName(), vf)));
						}
						for (int i=0; i<funcInfo.argumentTypes.length; i++) {
							BNode constraint = vf.createBNode();
							writer.handleStatement(vf.createStatement(funcIri, SPIN.CONSTRAINT_PROPERTY, constraint));
							writer.handleStatement(vf.createStatement(constraint, RDF.TYPE, SPL.ARGUMENT_TEMPLATE));
							writer.handleStatement(vf.createStatement(constraint, SPL.PREDICATE_PROPERTY, vf.createIRI(SP.NAMESPACE, "arg"+(i+1))));
							ItemType argType = funcInfo.argumentTypes[i].getPrimaryType();
							if (argType.isAtomicType()) {
								writer.handleStatement(vf.createStatement(constraint, SPL.VALUE_TYPE_PROPERTY, toIRI(argType.getAtomizedItemType().getTypeName(), vf)));
							}
						}
					}
				}
			}
			writer.endRDF();
		}
	}

	private static IRI toIRI(StructuredQName name, ValueFactory vf) {
		return vf.createIRI(name.getURI()+'#'+name.getLocalPart());
	}
}
