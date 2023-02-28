package com.msd.gin.halyard.common;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.Set;

public final class TableConfig {
    public static final String TABLE_VERSION = "halyard.table.version";
    static final int NO_VERSION = 0;
    static final int VERSION_4_6 = 460;
    static final int VERSION_4_6_1 = 461;
    static final int CURRENT_VERSION = VERSION_4_6_1;

    public static final String ID_HASH = "halyard.id.hash";
    public static final String ID_SIZE = "halyard.id.size";
    public static final String ID_TYPE_INDEX = "halyard.id.type.index";
    public static final String ID_TYPE_NIBBLE = "halyard.id.type.nibble";

    public static final String KEY_SIZE_SPO_SUBJECT = "halyard.key.spo.subject.size";
    public static final String KEY_SIZE_SPO_PREDICATE = "halyard.key.spo.predicate.size";
    public static final String KEY_SIZE_SPO_OBJECT = "halyard.key.spo.object.size";
    public static final String KEY_SIZE_SPO_CONTEXT = "halyard.key.spo.context.size";

    public static final String KEY_SIZE_POS_SUBJECT = "halyard.key.pos.subject.size";
    public static final String KEY_SIZE_POS_PREDICATE = "halyard.key.pos.predicate.size";
    public static final String KEY_SIZE_POS_OBJECT = "halyard.key.pos.object.size";
    public static final String KEY_SIZE_POS_CONTEXT = "halyard.key.pos.context.size";
    
    public static final String KEY_SIZE_OSP_SUBJECT = "halyard.key.osp.subject.size";
    public static final String KEY_SIZE_OSP_PREDICATE = "halyard.key.osp.predicate.size";
    public static final String KEY_SIZE_OSP_OBJECT = "halyard.key.osp.object.size";
    public static final String KEY_SIZE_OSP_CONTEXT = "halyard.key.osp.context.size";

    public static final String KEY_SIZE_CSPO_SUBJECT = "halyard.key.cspo.subject.size";
    public static final String KEY_SIZE_CSPO_PREDICATE = "halyard.key.cspo.predicate.size";
    public static final String KEY_SIZE_CSPO_OBJECT = "halyard.key.cspo.object.size";
    public static final String KEY_SIZE_CSPO_CONTEXT = "halyard.key.cspo.context.size";

    public static final String KEY_SIZE_CPOS_SUBJECT = "halyard.key.cpos.subject.size";
    public static final String KEY_SIZE_CPOS_PREDICATE = "halyard.key.cpos.predicate.size";
    public static final String KEY_SIZE_CPOS_OBJECT = "halyard.key.cpos.object.size";
    public static final String KEY_SIZE_CPOS_CONTEXT = "halyard.key.cpos.context.size";

    public static final String KEY_SIZE_COSP_SUBJECT = "halyard.key.cosp.subject.size";
    public static final String KEY_SIZE_COSP_PREDICATE = "halyard.key.cosp.predicate.size";
    public static final String KEY_SIZE_COSP_OBJECT = "halyard.key.cosp.object.size";
    public static final String KEY_SIZE_COSP_CONTEXT = "halyard.key.cosp.context.size";

    public static final String KEY_SIZE_SUBJECT = "halyard.key.subject.size";
    public static final String END_KEY_SIZE_SUBJECT = "halyard.endKey.subject.size";
    public static final String KEY_SIZE_PREDICATE = "halyard.key.predicate.size";
    public static final String END_KEY_SIZE_PREDICATE = "halyard.endKey.predicate.size";
    public static final String KEY_SIZE_OBJECT = "halyard.key.object.size";
    public static final String END_KEY_SIZE_OBJECT = "halyard.endKey.object.size";
    public static final String KEY_SIZE_CONTEXT = "halyard.key.context.size";
    public static final String END_KEY_SIZE_CONTEXT = "halyard.endKey.context.size";

	public static final String VOCABS = "halyard.vocabularies";
	public static final String NAMESPACES = "halyard.namespaces";
	public static final String NAMESPACE_PREFIXES = "halyard.namespacePrefixes";
	public static final String LANGS = "halyard.languages";
	public static final String STRING_COMPRESSION = "halyard.string.compressionThreshold";

	private static final Set<String> PROPERTIES;

	static {
		PROPERTIES = new HashSet<>();
		try {
			for (Field f : TableConfig.class.getFields()) {
				if (Modifier.isStatic(f.getModifiers())) {
					PROPERTIES.add((String) f.get(null));
				}
			}
		} catch (IllegalAccessException e) {
			throw new AssertionError(e);
		}
	}

	public static boolean contains(String prop) {
		return PROPERTIES.contains(prop);
	}

	private TableConfig() {}
}
