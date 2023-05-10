package com.msd.gin.halyard.optimizers;

import com.msd.gin.halyard.algebra.AbstractExtendedQueryModelVisitor;
import com.msd.gin.halyard.algebra.Algebra;
import com.msd.gin.halyard.algebra.ExtendedTupleFunctionCall;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.BooleanLiteral;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.algebra.Extension;
import org.eclipse.rdf4j.query.algebra.ExtensionElem;
import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.QueryRoot;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.ValueConstant;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.optimizer.QueryModelNormalizerOptimizer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TupleFunctionCallOptimizerTest {
	private StatementPattern createStatementPattern(String s, String p, String o) {
		return new StatementPattern(new Var(s), new Var(p), new Var(o));
	}

	private ExtendedTupleFunctionCall createTupleFunctionCall(String in, String out) {
		return createTupleFunctionCall("http://tuplefunctions/test", in, out);
	}

	private ExtendedTupleFunctionCall createTupleFunctionCall(String name, String in, String out) {
		return createTupleFunctionCall(name, new String[] {in}, new String[] {out});
	}

	private ExtendedTupleFunctionCall createTupleFunctionCall(String name, String[] inputVars, String[] outputVars) {
		ExtendedTupleFunctionCall tfc = new ExtendedTupleFunctionCall(name);
		if (inputVars != null) {
			tfc.setArgs(Arrays.asList(inputVars).stream().<ValueExpr>map(s -> new Var(s)).collect(Collectors.toList()));
		}
		tfc.setResultVars(Arrays.asList(outputVars).stream().map(s -> new Var(s)).collect(Collectors.toList()));
		return tfc;
	}

	private void optimize(TupleExpr root) {
		new TupleFunctionCallOptimizer().optimize(root, null, null);
		new QueryModelNormalizerOptimizer().optimize(root, null, null);
	}

	@Test
	public void testTupleFunction() {
        optimize(Algebra.ensureRooted(createTupleFunctionCall("in", "out")));
	}

	@Test
	public void testJoinWithLeftTupleFunction() {
		ExtendedTupleFunctionCall tfc = createTupleFunctionCall("o", "out");
		StatementPattern sp = createStatementPattern("s", "p", "o");
		Join join = new Join(tfc, sp);
		TupleExpr root = Algebra.ensureRooted(join);
		optimize(root);
		assertEquals(tfc, ((QueryRoot)root).getArg());
        assertEquals(sp, tfc.getDependentExpression(), root.toString());
	}

	@Test
	public void testJoinWithRightTupleFunction() {
		ExtendedTupleFunctionCall tfc = createTupleFunctionCall("o", "out");
		StatementPattern sp = createStatementPattern("s", "p", "o");
		Join join = new Join(sp, tfc);
		TupleExpr root = Algebra.ensureRooted(join);
		optimize(root);
		assertEquals(tfc, ((QueryRoot)root).getArg());
        assertEquals(sp, tfc.getDependentExpression(), root.toString());
	}

	@Test
	public void testFilterWithTupleFunction() {
		ExtendedTupleFunctionCall tfc = createTupleFunctionCall("o2", "out");
		StatementPattern sp1 = createStatementPattern("s", "p1", "o1");
		StatementPattern sp2 = createStatementPattern("s", "p2", "o2");
		Join join = new Join(sp2, new Filter(tfc, new ValueConstant(BooleanLiteral.TRUE)));
		Join topJoin = new Join(sp1, join);
		TupleExpr root = Algebra.ensureRooted(topJoin);
		optimize(root);
        List<Join> joins = getJoins(root);
        assertEquals(tfc, joins.get(0).getRightArg(), root.toString());
		assertEquals(sp2, tfc.getDependentExpression(), root.toString());
	}

	@Test
	public void testExtensionWithTupleFunction() {
		ValueFactory vf = SimpleValueFactory.getInstance();
		ExtendedTupleFunctionCall tfc = createTupleFunctionCall("o2", "out");
		StatementPattern sp = createStatementPattern("s", "p1", "o1");
		Extension ext = new Extension(sp, new ExtensionElem(new ValueConstant(vf.createLiteral("inValue")), "o2"));
		Join join = new Join(tfc, ext);
		TupleExpr root = Algebra.ensureRooted(join);
		optimize(root);
		assertEquals(tfc, ((QueryRoot)root).getArg());
        assertEquals(ext, tfc.getDependentExpression(), root.toString());
	}

	@Test
	public void testMultipleTupleFunctions() {
		ValueFactory vf = SimpleValueFactory.getInstance();
		ExtendedTupleFunctionCall tfcx = createTupleFunctionCall("http://func/X", null, new String[] {"v"});
		tfcx.setArgs(Arrays.asList(new ValueConstant(vf.createLiteral("a")), new ValueConstant(vf.createLiteral("b")), new ValueConstant(vf.createLiteral("c"))));
		ExtendedTupleFunctionCall tfc1 = createTupleFunctionCall("http://func/1", "v", "out1");
		ExtendedTupleFunctionCall tfc2 = createTupleFunctionCall("http://func/2", "v", "out2");
		ExtendedTupleFunctionCall tfc3 = createTupleFunctionCall("http://func/3", "v", "out3");
		Join join3 = new Join(tfc2, tfc3);
		Join join2 = new Join(tfc1, join3);
		Join join1 = new Join(tfcx, join2);
		TupleExpr root = Algebra.ensureRooted(join1);
		optimize(root);
		assertEquals(tfc2, tfc3.getDependentExpression(), root.toString());
		assertEquals(tfc1, tfc2.getDependentExpression(), root.toString());
		assertEquals(tfcx, tfc1.getDependentExpression(), root.toString());
	}

	@Test
	public void testTupleFunctionWithService() {
		ValueFactory vf = SimpleValueFactory.getInstance();
		ExtendedTupleFunctionCall tfc = createTupleFunctionCall("o1", "out");
		StatementPattern sp = createStatementPattern("s", "p1", "o1");
		Service service = new Service(new Var("url", vf.createIRI("http://endpoint")), sp, "", null, null, false);
		Join join = new Join(tfc, service);
		TupleExpr root = Algebra.ensureRooted(join);
		optimize(root);
		assertEquals(tfc, ((QueryRoot)root).getArg());
		assertEquals(service, tfc.getDependentExpression(), root.toString());
	}

	@Test
	public void testServiceWithTupleFunction() {
		ValueFactory vf = SimpleValueFactory.getInstance();
		ExtendedTupleFunctionCall tfc = createTupleFunctionCall("o1", "out");
		StatementPattern sp = createStatementPattern("s", "p1", "o1");
		Service service = new Service(new Var("url", vf.createIRI("http://endpoint")), tfc, "", null, null, false);
		Join join = new Join(service, sp);
		TupleExpr root = Algebra.ensureRooted(join);
		optimize(root);
		assertEquals(service, ((QueryRoot)root).getArg());
		assertEquals(sp, tfc.getDependentExpression(), root.toString());
	}

	private List<Join> getJoins(TupleExpr expr) {
		List<Join> joins = new ArrayList<>();
		expr.visit(new AbstractExtendedQueryModelVisitor<RuntimeException>() {
			@Override
			public void meet(Join node) throws RuntimeException {
				joins.add(node);
				super.meet(node);
			}
		});
		return joins;
	}
}
