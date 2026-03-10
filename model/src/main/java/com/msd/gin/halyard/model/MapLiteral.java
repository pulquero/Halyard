package com.msd.gin.halyard.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.json.JSONObject;

import com.msd.gin.halyard.model.vocabulary.HALYARD;
import com.msd.gin.halyard.model.vocabulary.HalyardDatatype;

public final class MapLiteral extends AbstractDataLiteral implements ObjectLiteral<Map<String,Object>> {
	private static final long serialVersionUID = -8130963762756953874L;

	public static boolean isMapLiteral(Value v) {
		return v != null && v.isLiteral() && isMapLiteral((Literal)v);
	}

	public static boolean isMapLiteral(Literal l) {
		return (l.getCoreDatatype() == HalyardDatatype.MAP) || HALYARD.MAP_TYPE.equals(l.getDatatype());
	}

	public static Map<String,Object> objectMap(Literal l) {
		if (l instanceof MapLiteral) {
			return ((MapLiteral)l).map;
		} else {
			return parse(l.getLabel());
		}
	}

	private static Map<String,Object> parse(CharSequence s) {
		JSONObject obj = new JSONObject(s);
		Map<String,Object> map = new HashMap<>(obj.length()+1);
		for (String k : (Set<String>) obj.keySet()) {
			map.put(k, obj.get(k));
		}
		return map;
	}

	private final Map<String,Object> map;

	public MapLiteral(String s) {
		this.map = parse(s);
	}

	public MapLiteral(Map<String,Object> map) {
		this.map = map;
	}

	@Override
	public String getLabel() {
		JSONObject obj = new JSONObject();
		for (Map.Entry<String,Object> entry : this.map.entrySet()) {
			obj.put(entry.getKey(), entry.getValue());
		}
		return obj.toString(0);
	}

	@Override
	public IRI getDatatype() {
		return HalyardDatatype.MAP.getIri();
	}

	@Override
	public CoreDatatype getCoreDatatype() {
		return HalyardDatatype.MAP;
	}

	@Override
	public Map<String,Object> objectValue() {
		return map;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o instanceof MapLiteral) {
			MapLiteral other = (MapLiteral) o;
			return map.equals(other.map);
		} else {
			return super.equals(o);
		}
	}
}
