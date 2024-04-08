package com.msd.gin.halyard.query.algebra;

import java.util.stream.Stream;

import org.eclipse.rdf4j.query.algebra.BinaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.VariableScopeChange;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;

public class ExtendedQueryModelTreePrinter extends AbstractQueryModelVisitor<RuntimeException> {
	private static final String LINE_SEPARATOR = System.getProperty("line.separator");

	private final String indentString = "   ";
	private final StringBuilder sb;
	private int indentLevel = 0;

	public ExtendedQueryModelTreePrinter() {
		sb = new StringBuilder(256);
	}

	@Override
	protected void meetNode(QueryModelNode node) {
		for (int i = 0; i < indentLevel; i++) {
			sb.append(indentString);
		}

		sb.append(node.getSignature());

		if (node instanceof VariableScopeChange) {
			if (((VariableScopeChange) node).isVariableScopeChange()) {
				sb.append(" (new scope)");
			}
		}

		if (node instanceof BinaryTupleOperator) {
			String algorithmName = ((BinaryTupleOperator) node).getAlgorithmName();
			if (algorithmName != null) {
				sb.append(" (").append(algorithmName).append(")");
			}
		} else if (node instanceof NAryTupleOperator) {
			String algorithmName = ((NAryTupleOperator) node).getAlgorithmName();
			if (algorithmName != null) {
				sb.append(" (").append(algorithmName).append(")");
			}
		}

		appendCostAnnotation(node, sb);
		sb.append(LINE_SEPARATOR);

		indentLevel++;

		super.meetNode(node);

		indentLevel--;
	}

	public String getTreeString() {
		return sb.toString();
	}

	/**
	 * @return Human readable number. Eg. 12.1M for 1212213.4 and UNKNOWN for -1.
	 */
	static String toHumanReadableNumber(double number) {
		String humanReadbleString;
		if (number == Double.POSITIVE_INFINITY) {
			humanReadbleString = "∞";
		} else if (number > 1e18) {
			humanReadbleString = Math.round(number / 1e17) / 10.0 + "E";
		} else if (number > 1e15) {
			humanReadbleString = Math.round(number / 1e14) / 10.0 + "P";
		} else if (number > 1e12) {
			humanReadbleString = Math.round(number / 1e11) / 10.0 + "T";
		} else if (number > 1e9) {
			humanReadbleString = Math.round(number / 1e8) / 10.0 + "G";
		} else if (number > 1e6) {
			humanReadbleString = Math.round(number / 1e5) / 10.0 + "M";
		} else if (number > 1e3) {
			humanReadbleString = Math.round(number / 1e2) / 10.0 + "k";
		} else if (number >= 0) {
			humanReadbleString = Math.round(number) + "";
		} else {
			humanReadbleString = "UNKNOWN";
		}

		return humanReadbleString;
	}

	/**
	 * @return Human readable time.
	 */
	static String toHumanReadableTime(long nanos) {
		String humanReadbleString;

		if (nanos > 1_000_000_000) {
			humanReadbleString = nanos / 100_000_000 / 10.0 + "s";
		} else if (nanos > 1_000_000) {
			humanReadbleString = nanos / 100_000 / 10.0 + "ms";
		} else if (nanos >= 1000) {
			humanReadbleString = nanos / 1000 / 1000.0 + "ms";
		} else if (nanos >= 0) {
			humanReadbleString = nanos + "ns";
		} else {
			humanReadbleString = "UNKNOWN";
		}

		return humanReadbleString;
	}

	private static void appendCostAnnotation(QueryModelNode node, StringBuilder sb) {
		String costs = Stream.of(
				"costEstimate=" + toHumanReadableNumber(node.getCostEstimate()),
				"resultSizeEstimate=" + toHumanReadableNumber(node.getResultSizeEstimate()),
				"resultSizeActual=" + toHumanReadableNumber(node.getResultSizeActual()),
				"totalTimeActual=" + toHumanReadableTime(node.getTotalTimeNanosActual()))
				.filter(s -> !s.endsWith("UNKNOWN"))
				.reduce((a, b) -> a + ", " + b)
				.orElse("");

		if (!costs.isEmpty()) {
			sb.append(" (").append(costs).append(")");
		}
	}
}
