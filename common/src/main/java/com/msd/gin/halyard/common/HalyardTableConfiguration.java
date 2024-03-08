package com.msd.gin.halyard.common;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.Maps;
import com.msd.gin.halyard.model.vocabulary.IRIEncodingNamespace;
import com.msd.gin.halyard.model.vocabulary.Vocabulary;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.model.util.Vocabularies;
import org.eclipse.rdf4j.model.vocabulary.DC;
import org.eclipse.rdf4j.model.vocabulary.DCTERMS;
import org.eclipse.rdf4j.model.vocabulary.FOAF;
import org.eclipse.rdf4j.model.vocabulary.GEO;
import org.eclipse.rdf4j.model.vocabulary.ORG;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.model.vocabulary.PROV;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.model.vocabulary.ROV;
import org.eclipse.rdf4j.model.vocabulary.SD;
import org.eclipse.rdf4j.model.vocabulary.SKOS;
import org.eclipse.rdf4j.model.vocabulary.SKOSXL;
import org.eclipse.rdf4j.model.vocabulary.VOID;
import org.eclipse.rdf4j.model.vocabulary.WGS84;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Persistable configuration only.
 * Immutable.
 */
final class HalyardTableConfiguration {
	private static final Logger LOGGER = LoggerFactory.getLogger(HalyardTableConfiguration.class);
	private static final List<? extends Class<?>> VOCAB_IMPLS = getVocabularies();
	private static final Map<String, Namespace> NAMESPACE_IMPLS = getNamespaces(VOCAB_IMPLS);

	private final Map<String,String> config = new HashMap<>();
	private final BiMap<Integer, IdentifiableIRI> wellKnownIris;
	private final BiMap<Short, String> wellKnownNamespaces;
	private final Map<String,Namespace> wellKnownNamespacePrefixes;
	private final Map<Short, IRIEncodingNamespace> iriEncoders;
	private final BiMap<Short, String> wellKnownLangs;
	private final int hashCode;

	private static List<? extends Class<?>> getVocabularies() {
		List<Class<?>> vocabs = new ArrayList<>(25);

		LOGGER.info("Loading default vocabularies...");
		Class<?>[] defaultVocabClasses = { RDF.class, RDFS.class, XSD.class, SD.class, VOID.class, FOAF.class,
				OWL.class, DC.class, DCTERMS.class, SKOS.class, SKOSXL.class, ORG.class, GEO.class,
				WGS84.class, PROV.class, ROV.class };
		for (Class<?> vocabClass : defaultVocabClasses) {
			LOGGER.debug("Loading vocabulary {}", vocabClass.getName());
			vocabs.add(vocabClass);
		}

		LOGGER.info("Searching for additional vocabularies...");
		for (Vocabulary vocab : ServiceLoader.load(Vocabulary.class)) {
			LOGGER.info("Loading vocabulary {}", vocab.getClass().getName());
			vocabs.add(vocab.getClass());
		}

		return Collections.unmodifiableList(vocabs);
	}

	private static Set<IRI> getIRIs(Collection<? extends Class<?>> vocabs) {
		Set<IRI> iris = new HashSet<>();
		for (Class<?> vocab : vocabs) {
			iris.addAll(Vocabularies.getIRIs(vocab));
			try {
				Method getIRIs = vocab.getMethod("getIRIs");
				iris.addAll((Collection<IRI>) getIRIs.invoke(null));
			} catch (NoSuchMethodException e) {
			} catch (IllegalAccessException e) {
				throw new RuntimeException(e);
			} catch (InvocationTargetException e) {
				throw new RuntimeException(e.getCause());
			}
		}
		return iris;
	}

	private static Map<String,Namespace> getNamespaces(Collection<? extends Class<?>> vocabs) {
		List<Namespace> namespaceImpls = new ArrayList<>();
		for (Class<?> vocab : vocabs) {
			for (Field f : vocab.getFields()) {
				if (f.getType() == Namespace.class) {
					try {
						Namespace ns = (Namespace) f.get(null);
						namespaceImpls.add(ns);
					} catch (IllegalAccessException ex) {
						throw new AssertionError(ex);
					}
				}
			}
			try {
				Method getNamespaces = vocab.getMethod("getNamespaces");
				namespaceImpls.addAll((Collection<Namespace>) getNamespaces.invoke(null));
			} catch (NoSuchMethodException e) {
			} catch (IllegalAccessException e) {
				throw new RuntimeException(e);
			} catch (InvocationTargetException e) {
				throw new RuntimeException(e.getCause());
			}
		}

		Map<String,Namespace> namespaces = new HashMap<>(namespaceImpls.size()+1);
		for (Namespace ns : namespaceImpls) {
			String name = ns.getName();
			if (namespaces.putIfAbsent(name, ns) != null) {
				throw new AssertionError(String.format("Multiple namespace implementations for %s: %s, %s",
						name, namespaces.get(name).getClass().getName(), ns.getClass().getName()));
			}
		}
		return namespaces;
	}

	private static void copyTableConfig(Iterable<Map.Entry<String,String>> conf, Map<String,String> map) {
		for (Map.Entry<String, String> entry : conf) {
			String prop = entry.getKey();
			if (TableConfig.contains(prop)) {
				map.put(prop, entry.getValue());
			}
		}
	}

	HalyardTableConfiguration(Iterable<Map.Entry<String,String>> conf) {
		config.put(TableConfig.TABLE_VERSION, String.valueOf(TableConfig.CURRENT_VERSION));
		Configuration defaultConf = new Configuration(false);
		defaultConf.addResource(TableConfig.class.getResource("default-config.xml"));
		copyTableConfig(defaultConf, config);
		copyTableConfig(conf, config);

		String vocabs = config.get(TableConfig.VOCABS);
		boolean loadVocabs = Boolean.parseBoolean(vocabs);
		if (!loadVocabs) {
			JSONObject vocabJson = null;
			if (vocabs != null) {
				try {
					vocabJson = new JSONObject(vocabs);
				} catch (JSONException jsonex) {
					// false value
				}
			}
			if (vocabJson != null) {
				wellKnownIris = HashBiMap.create(1024);
				for (String iri : (Set<String>) vocabJson.keySet()) {
					Integer hash = vocabJson.getInt(iri);
					addIRI(hash, new IdentifiableIRI(iri));
				}
			} else {
				wellKnownIris = ImmutableBiMap.of();
			}
		} else {
			wellKnownIris = HashBiMap.create(1024);
			for (IRI iri : getIRIs(VOCAB_IMPLS)) {
				String s = iri.stringValue();
				Integer hash = Hashes.hash32(Hashes.toBytes(s));
				addIRI(hash, new IdentifiableIRI(s));
			}
			config.put(TableConfig.VOCABS, JSONObject.valueToString(wellKnownIris.inverse()));
		}

		String namespaces = config.get(TableConfig.NAMESPACES);
		if (namespaces != null) {
			wellKnownNamespaces = HashBiMap.create(256);
			JSONObject nsJson = new JSONObject(namespaces);
			for (String ns : (Set<String>) nsJson.keySet()) {
				Short hash = (short) nsJson.getInt(ns);
				addNamespace(hash, ns);
			}
		} else {
			if (loadVocabs) {
				wellKnownNamespaces = HashBiMap.create(256);
				for (Namespace namespace : NAMESPACE_IMPLS.values()) {
					String name = namespace.getName();
					Short hash = Hashes.hash16(Hashes.toBytes(name));
					addNamespace(hash, name);
				}
				config.put(TableConfig.NAMESPACES, JSONObject.valueToString(wellKnownNamespaces.inverse()));
			} else {
				wellKnownNamespaces = ImmutableBiMap.of();
			}
		}

		// lookup IRI encoders for the configured namespaces
		iriEncoders = new HashMap<>(wellKnownNamespaces.size()+1);
		for (Map.Entry<Short,String> nsEntry : wellKnownNamespaces.entrySet()) {
			Namespace ns = NAMESPACE_IMPLS.get(nsEntry.getValue());
			if (ns instanceof IRIEncodingNamespace) {
				iriEncoders.put(nsEntry.getKey(), (IRIEncodingNamespace) ns);
			}
		}

		String prefixes = config.get(TableConfig.NAMESPACE_PREFIXES);
		if (prefixes != null) {
			wellKnownNamespacePrefixes = new HashMap<>(256);
			JSONObject prefixJson = new JSONObject(prefixes);
			for (String prefix : (Set<String>) prefixJson.keySet()) {
				String ns = prefixJson.getString(prefix);
				addNamespacePrefix(new SimpleNamespace(prefix, ns));
			}
		} else {
			if (loadVocabs) {
				wellKnownNamespacePrefixes = new HashMap<>(256);
				for (Namespace namespace : NAMESPACE_IMPLS.values()) {
					addNamespacePrefix(namespace);
				}
				config.put(TableConfig.NAMESPACE_PREFIXES, JSONObject.valueToString(Maps.transformValues(wellKnownNamespacePrefixes, ns -> ns.getName())));
			} else {
				wellKnownNamespacePrefixes = Collections.emptyMap();
			}
		}

		String langs = config.get(TableConfig.LANGS);
		boolean loadLangs = Boolean.parseBoolean(langs);
		if (!loadLangs) {
			JSONObject langJson = null;
			if (langs != null) {
				try {
					langJson = new JSONObject(langs);
				} catch (JSONException jsonex) {
					// false value
				}
			}
			if (langJson != null) {
				wellKnownLangs = HashBiMap.create(256);
				for (String langTag : (Set<String>) langJson.keySet()) {
					Short hash = (short) langJson.getInt(langTag);
					addLanguageTag(hash, langTag);
				}
			} else {
				wellKnownLangs = ImmutableBiMap.of();
			}
		} else {
			wellKnownLangs = HashBiMap.create(256);
			try {
				try (BufferedReader reader = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("languageTags"), "US-ASCII"))) {
					reader.lines().forEach(langTag -> {
						LOGGER.debug("Loading language {}", langTag);
						Short hash = Hashes.hash16(Hashes.toBytes(langTag));
						addLanguageTag(hash, langTag);
						// add lowercase alternative
						String lcLangTag = langTag.toLowerCase();
						if (!lcLangTag.equals(langTag)) {
							Short lcHash = Hashes.hash16(Hashes.toBytes(lcLangTag));
							addLanguageTag(lcHash, lcLangTag);
						}
					});
				}
			} catch (IOException ioe) {
				throw new RuntimeException(ioe);
			}
		}

		hashCode = config.hashCode();
	}

	private void addIRI(Integer hash, IdentifiableIRI iri) {
		if (wellKnownIris.putIfAbsent(hash, iri) != null) {
			throw new AssertionError(String.format("IRI hash collision between %s and %s",
					wellKnownIris.get(hash), iri));
		}
	}

	private void addNamespace(Short hash, String name) {
		if (wellKnownNamespaces.putIfAbsent(hash, name) != null) {
			throw new AssertionError(String.format("Namespace hash collision between %s and %s",
					wellKnownNamespaces.get(hash), name));
		}
	}

	private void addNamespacePrefix(Namespace ns) {
		if (wellKnownNamespacePrefixes.put(ns.getPrefix(), ns) != null) {
			throw new AssertionError(String.format("Namespace prefix collision between %s and %s",
					wellKnownNamespacePrefixes.get(ns.getPrefix()), ns.getName()));
		}
	}

	private void addLanguageTag(Short hash, String langTag) {
		if (wellKnownLangs.putIfAbsent(hash, langTag) != null) {
			throw new AssertionError(String.format("Language tag hash collision between %s and %s",
					wellKnownLangs.get(hash), langTag));
		}
	}


	public Integer getWellKnownIRIHash(IRI iri) {
		return wellKnownIris.inverse().get(iri);
	}

	public IRI getWellKnownIRI(int hash) {
		return wellKnownIris.get(hash);
	}

	public String getWellKnownNamespace(short hash) {
		return wellKnownNamespaces.get(hash);
	}

	public Short getWellKnownNamespaceHash(String ns) {
		return wellKnownNamespaces.inverse().get(ns);
	}

	public IRIEncodingNamespace getIRIEncodingNamespace(short hash) {
		return iriEncoders.get(hash);
	}

	public String getWellKnownLanguageTag(short hash) {
		return wellKnownLangs.get(hash);
	}

	public Short getWellKnownLanguageTagHash(String lt) {
		return wellKnownLangs.inverse().get(lt);
	}

	public Collection<? extends Namespace> getWellKnownNamespaces() {
		return wellKnownNamespacePrefixes.values();
	}

	public Collection<? extends IdentifiableIRI> getWellKnownIRIs() {
		return wellKnownIris.values();
	}


	String get(String name) {
		return config.get(name);
	}

	boolean getBoolean(String name) {
		return Boolean.parseBoolean(config.get(name));
	}

	int getInt(String name) {
		return Integer.parseInt(config.get(name));
	}

	int getInt(String name, int defaultValue) {
		String value = config.get(name);
		return (value != null) ? Integer.parseInt(value) : defaultValue;
	}

	void writeXml(OutputStream out) throws IOException {
		Configuration conf = new Configuration(false);
		conf.addResource(TableConfig.class.getResource("default-config.xml"));
		for (Map.Entry<String, String> entry : config.entrySet()) {
			conf.set(entry.getKey(), entry.getValue());
		}
		conf.writeXml(out);
	}

	@Override
	public int hashCode() {
		return hashCode;
	}

	@Override
	public boolean equals(Object other) {
		if (this == other) {
			return true;
		}
		if (other == null || this.getClass() != other.getClass()) {
			return false;
		}
		HalyardTableConfiguration that = (HalyardTableConfiguration) other;
		// use hash code as a bloom filter
		return (this.hashCode == that.hashCode) && this.config.equals(that.config);
	}
}
