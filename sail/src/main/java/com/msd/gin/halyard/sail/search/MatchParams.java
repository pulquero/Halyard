package com.msd.gin.halyard.sail.search;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

final class MatchParams implements Serializable {
	private static final long serialVersionUID = -1524678402469442919L;

	final List<String> valueVars = new ArrayList<>(1);
	final List<String> scoreVars = new ArrayList<>(1);
	final List<String> indexVars = new ArrayList<>(1);
	final List<MatchParams.FieldParams> fields = new ArrayList<>(1);

	boolean isValid() {
		return !valueVars.isEmpty() || !scoreVars.isEmpty() || !indexVars.isEmpty() || !fields.isEmpty();
	}

	@Override
	public boolean equals(Object o) {
		if (o == this) {
			return true;
		}
		if (o instanceof MatchParams) {
			MatchParams other = (MatchParams) o;
			return Objects.equals(valueVars, other.valueVars) && Objects.equals(scoreVars, other.scoreVars) && Objects.equals(indexVars, other.indexVars);
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return Objects.hash(valueVars, scoreVars, indexVars);
	}


	static final class FieldParams implements Serializable {
		private static final long serialVersionUID = -326151903390393152L;

		String name;
		final List<String> valueVars = new ArrayList<>(1);

		boolean isValid() {
			return (name != null) && !valueVars.isEmpty();
		}

		@Override
		public boolean equals(Object o) {
			if (o == this) {
				return true;
			}
			if (o instanceof MatchParams.FieldParams) {
				MatchParams.FieldParams other = (MatchParams.FieldParams) o;
				return Objects.equals(name, other.name) && Objects.equals(valueVars, other.valueVars);
			} else {
				return false;
			}
		}

		@Override
		public int hashCode() {
			return Objects.hash(name, valueVars);
		}
	}
}