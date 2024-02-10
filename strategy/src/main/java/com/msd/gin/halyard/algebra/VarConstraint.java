package com.msd.gin.halyard.algebra;

import com.msd.gin.halyard.common.ValueType;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.eclipse.rdf4j.query.algebra.Compare.CompareOp;
import org.eclipse.rdf4j.query.algebra.ValueExpr;

public final class VarConstraint implements Serializable, Cloneable {
	private static final long serialVersionUID = -2775955938552284845L;

	private ValueType valueType;
	private FunctionalConstraint functionalConstraint;
	private int partitionCount;

	public VarConstraint(ValueType t) {
		this(t, null, 0);
	}

	public VarConstraint(ValueType t, ValueExpr func, CompareOp op, ValueExpr value) {
		this(t, new FunctionalConstraint(func, op, value), 0);
	}

	public static VarConstraint partitionConstraint(int partitionCount) {
		return new VarConstraint(null, null, partitionCount);
	}

	private VarConstraint(ValueType t, FunctionalConstraint func, int partitionCount) {
		this.valueType = t;
		this.functionalConstraint = func;
		this.partitionCount = partitionCount;
	}

	public ValueType getValueType() {
		return valueType;
	}

	public FunctionalConstraint getFunctionalConstraint() {
		return functionalConstraint;
	}

	public int getPartitionCount() {
		return partitionCount;
	}

	public boolean isPartitioned() {
		return (partitionCount > 0);
	}

	@Override
	public boolean equals(Object other) {
		if (other instanceof VarConstraint) {
			VarConstraint o = (VarConstraint) other;
			return this.valueType == o.valueType
					&& Objects.equals(this.functionalConstraint, o.functionalConstraint)
					&& this.partitionCount == o.partitionCount;
		}
		return false;
	}

	@Override
	public int hashCode() {
		int result = super.hashCode();
		result = 89 * result + Objects.hashCode(valueType);
		result = 89 * result + Objects.hashCode(functionalConstraint);
		result = 89 * result + partitionCount;
		return result;
	}

	@Override
	public VarConstraint clone() {
		try {
			VarConstraint clone = (VarConstraint) super.clone();
			if (functionalConstraint != null) {
				clone.functionalConstraint = functionalConstraint.clone();
			}
			return clone;
		} catch(CloneNotSupportedException ex) {
			throw new AssertionError(ex);
		}
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(64);
		String sep = "";
		if (valueType != null) {
			sb.append(valueType);
			sep = ", ";
		}
		if (functionalConstraint != null) {
			sb.append(sep).append(functionalConstraint);
			sep = ", ";
		}
		if (partitionCount != 0) {
			sb.append(sep).append("partitioned by ").append(partitionCount);
		}
		return sb.toString();
	}

	public static VarConstraint merge(VarConstraint a, VarConstraint b) {
		Optional<Optional<ValueType>> newVT = nonNull(a.valueType, b.valueType);
		if (newVT.isEmpty()) {
			return null;
		}
		Optional<Optional<FunctionalConstraint>> newFunc = nonNull(a.functionalConstraint, b.functionalConstraint);
		if (newFunc.isEmpty()) {
			return null;
		}
		int newPC = nonZero(a.partitionCount, b.partitionCount);
		if (newPC == -1) {
			return null;
		}
		return new VarConstraint(newVT.get().orElse(null), newFunc.get().orElse(null), newPC);
	}

	private static <E> Optional<Optional<E>> nonNull(E a, E b) {
		if (a != null && b == null) {
			return Optional.of(Optional.of(a));
		} else if (a == null && b != null) {
			return Optional.of(Optional.of(b));
		} else if (Objects.equals(a, b)) {
			return Optional.of(Optional.ofNullable(a));
		} else {
			// not compatible
			return Optional.empty();
		}
	}

	private static int nonZero(int a, int b) {
		if (a != 0 && b == 0) {
			return a;
		} else if (a == 0 && b != 0) {
			return b;
		} else if (a == b) {
			return a;
		} else {
			// not compatible
			return -1;
		}
	}


	public static final class FunctionalConstraint implements Serializable, Cloneable {
		private static final long serialVersionUID = -7417409270097967448L;

		private ValueExpr function;
		private CompareOp op;
		private ValueExpr value;

		public FunctionalConstraint(@Nonnull ValueExpr f, @Nonnull CompareOp op, @Nonnull ValueExpr v) {
			this.function = f;
			this.op = op;
			this.value = v;
		}

		public ValueExpr getFunction() {
			return function;
		}

		public CompareOp getOp() {
			return op;
		}

		public ValueExpr getValue() {
			return value;
		}

		@Override
		public boolean equals(Object other) {
			if (other instanceof FunctionalConstraint) {
				FunctionalConstraint o = (FunctionalConstraint) other;
				return this.function.equals(o.function)
						&& this.op == o.op
						&& this.value.equals(o.value);
			}
			return false;
		}

		@Override
		public int hashCode() {
			int result = super.hashCode();
			result = 89 * result + function.hashCode();
			result = 89 * result + op.hashCode();
			result = 89 * result + value.hashCode();
			return result;
		}

		@Override
		public FunctionalConstraint clone() {
			try {
				FunctionalConstraint clone = (FunctionalConstraint) super.clone();
				clone.function = function.clone();
				clone.value = value.clone();
				return clone;
			} catch(CloneNotSupportedException ex) {
				throw new AssertionError(ex);
			}
		}

		@Override
		public String toString() {
			return function.getSignature() + " " + op.getSymbol() + " " + value.getSignature();
		}
	}
}
