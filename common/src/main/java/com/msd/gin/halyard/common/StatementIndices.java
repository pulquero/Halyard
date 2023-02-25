package com.msd.gin.halyard.common;

import static com.msd.gin.halyard.common.StatementIndex.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.ValueFactory;

public final class StatementIndices {
	private final int maxCaching;
	private final RDFFactory rdfFactory;
	private final StatementIndex<SPOC.S,SPOC.P,SPOC.O,SPOC.C> spo;
	private final StatementIndex<SPOC.P,SPOC.O,SPOC.S,SPOC.C> pos;
	private final StatementIndex<SPOC.O,SPOC.S,SPOC.P,SPOC.C> osp;
	private final StatementIndex<SPOC.C,SPOC.S,SPOC.P,SPOC.O> cspo;
	private final StatementIndex<SPOC.C,SPOC.P,SPOC.O,SPOC.S> cpos;
	private final StatementIndex<SPOC.C,SPOC.O,SPOC.S,SPOC.P> cosp;

	public static StatementIndices create() {
		Configuration conf = HBaseConfiguration.create();
		return new StatementIndices(conf, RDFFactory.create(conf));
	}

	public StatementIndices(Configuration conf, RDFFactory rdfFactory) {
        this.maxCaching = conf.getInt(HConstants.HBASE_CLIENT_SCANNER_CACHING, HConstants.DEFAULT_HBASE_CLIENT_SCANNER_CACHING);
		this.rdfFactory = rdfFactory;

		this.spo = new StatementIndex<>(
			StatementIndex.Name.SPO, 0,
			rdfFactory.getSubjectRole(StatementIndex.Name.SPO),
			rdfFactory.getPredicateRole(StatementIndex.Name.SPO),
			rdfFactory.getObjectRole(StatementIndex.Name.SPO),
			rdfFactory.getContextRole(StatementIndex.Name.SPO),
			rdfFactory, conf
		);
		this.pos = new StatementIndex<>(
			StatementIndex.Name.POS, 1,
			rdfFactory.getPredicateRole(StatementIndex.Name.POS),
			rdfFactory.getObjectRole(StatementIndex.Name.POS),
			rdfFactory.getSubjectRole(StatementIndex.Name.POS),
			rdfFactory.getContextRole(StatementIndex.Name.POS),
			rdfFactory, conf
		);
		this.osp = new StatementIndex<>(
			StatementIndex.Name.OSP, 2,
			rdfFactory.getObjectRole(StatementIndex.Name.OSP),
			rdfFactory.getSubjectRole(StatementIndex.Name.OSP),
			rdfFactory.getPredicateRole(StatementIndex.Name.OSP),
			rdfFactory.getContextRole(StatementIndex.Name.OSP),
			rdfFactory, conf
		);
		this.cspo = new StatementIndex<>(
			StatementIndex.Name.CSPO, 3,
			rdfFactory.getContextRole(StatementIndex.Name.CSPO),
			rdfFactory.getSubjectRole(StatementIndex.Name.CSPO),
			rdfFactory.getPredicateRole(StatementIndex.Name.CSPO),
			rdfFactory.getObjectRole(StatementIndex.Name.CSPO),
			rdfFactory, conf
		);
		this.cpos = new StatementIndex<>(
			StatementIndex.Name.CPOS, 4,
			rdfFactory.getContextRole(StatementIndex.Name.CPOS),
			rdfFactory.getPredicateRole(StatementIndex.Name.CPOS),
			rdfFactory.getObjectRole(StatementIndex.Name.CPOS),
			rdfFactory.getSubjectRole(StatementIndex.Name.CPOS),
			rdfFactory, conf
		);
		this.cosp = new StatementIndex<>(
			StatementIndex.Name.COSP, 5,
			rdfFactory.getContextRole(StatementIndex.Name.COSP),
			rdfFactory.getObjectRole(StatementIndex.Name.COSP),
			rdfFactory.getSubjectRole(StatementIndex.Name.COSP),
			rdfFactory.getPredicateRole(StatementIndex.Name.COSP),
			rdfFactory, conf
		);
	}

	public RDFFactory getRDFFactory() {
		return rdfFactory;
	}

	public StatementIndex<SPOC.S,SPOC.P,SPOC.O,SPOC.C> getSPOIndex() {
		return spo;
	}

	public StatementIndex<SPOC.P,SPOC.O,SPOC.S,SPOC.C> getPOSIndex() {
		return pos;
	}

	public StatementIndex<SPOC.O,SPOC.S,SPOC.P,SPOC.C> getOSPIndex() {
		return osp;
	}

	public StatementIndex<SPOC.C,SPOC.S,SPOC.P,SPOC.O> getCSPOIndex() {
		return cspo;
	}

	public StatementIndex<SPOC.C,SPOC.P,SPOC.O,SPOC.S> getCPOSIndex() {
		return cpos;
	}

	public StatementIndex<SPOC.C,SPOC.O,SPOC.S,SPOC.P> getCOSPIndex() {
		return cosp;
	}

	public StatementIndex<?,?,?,?> toIndex(byte prefix) {
		switch(prefix) {
			case 0: return spo;
			case 1: return pos;
			case 2: return osp;
			case 3: return cspo;
			case 4: return cpos;
			case 5: return cosp;
			default: throw new AssertionError(String.format("Invalid prefix: %s", prefix));
		}
	}

	public Scan scanAll() {
		int cardinality = VAR_CARDINALITY*VAR_CARDINALITY*VAR_CARDINALITY*VAR_CARDINALITY*VAR_CARDINALITY;
        int rowBatchSize = Math.min(maxCaching, cardinality);
		return HalyardTableUtils.scan(
			spo.concat(false, spo.role1.startKey(), spo.role2.startKey(), spo.role3.startKey(), spo.role4.startKey()),
			cosp.concat(true, cosp.role1.stopKey(), cosp.role2.stopKey(), cosp.role3.stopKey(), cosp.role4.stopKey()),
			rowBatchSize,
			true
		);
	}

	public Scan scanDefaultIndices() {
		int cardinality = VAR_CARDINALITY*VAR_CARDINALITY*VAR_CARDINALITY*VAR_CARDINALITY*VAR_CARDINALITY;
        int rowBatchSize = Math.min(maxCaching, cardinality);
		return HalyardTableUtils.scan(
			spo.concat(false, spo.role1.startKey(), spo.role2.startKey(), spo.role3.startKey(), spo.role4.startKey()),
			osp.concat(true, osp.role1.stopKey(), osp.role2.stopKey(), osp.role3.stopKey(), osp.role4.stopKey()),
			rowBatchSize,
			true
		);
	}

	public List<Scan> scanContextIndices(Resource graph) {
		List<Scan> scans = new ArrayList<>(3);
		RDFContext ctx = rdfFactory.createContext(graph);
		List<StatementIndex<SPOC.C,?,?,?>> ctxIndices = Arrays.asList(cspo, cpos, cosp);
		for (StatementIndex<SPOC.C,?,?,?> index : ctxIndices) {
			scans.add(index.scan(ctx));
		}
		return scans;
	}

	public Scan scanLiterals(IRI predicate, Resource graph) {
		if (predicate != null && graph != null) {
			return scanPredicateGraphLiterals(predicate, graph);
		} else if (predicate != null && graph == null) {
			return scanPredicateLiterals(predicate);
		} else if (predicate == null && graph != null) {
			return scanGraphLiterals(graph);
		} else {
			return scanAllLiterals();
		}
	}

	private Scan scanAllLiterals() {
		StatementIndex<SPOC.O,SPOC.S,SPOC.P,SPOC.C> index = osp;
		int typeSaltSize = rdfFactory.typeSaltSize;
		if (typeSaltSize == 1) {
			int cardinality = VAR_CARDINALITY*VAR_CARDINALITY*VAR_CARDINALITY*VAR_CARDINALITY;
			return index.scan(
				rdfFactory.writeSaltAndType(0, ValueType.LITERAL, null, index.role1.startKey()), index.role2.startKey(), index.role3.startKey(), index.role4.startKey(),
				rdfFactory.writeSaltAndType(0, ValueType.LITERAL, null, index.role1.stopKey()), index.role2.stopKey(), index.role3.stopKey(), index.role4.stopKey(),
				cardinality,
				true
			);
		} else {
			return index.scanWithConstraint(new ValueConstraint(ValueType.LITERAL));
		}
	}

	private Scan scanGraphLiterals(Resource graph) {
		RDFContext ctx = rdfFactory.createContext(graph);
		StatementIndex<SPOC.C,SPOC.O,SPOC.S,SPOC.P> index = cosp;
		int typeSaltSize = rdfFactory.typeSaltSize;
		if (typeSaltSize == 1) {
			ByteSequence ctxb = new ByteArray(index.role1.keyHash(ctx.getId()));
			int cardinality = VAR_CARDINALITY*VAR_CARDINALITY*VAR_CARDINALITY;
			return index.scan(
				ctxb, rdfFactory.writeSaltAndType(0, ValueType.LITERAL, null, index.role2.startKey()), index.role3.startKey(), index.role4.startKey(),
				ctxb, rdfFactory.writeSaltAndType(0, ValueType.LITERAL, null, index.role2.stopKey()), index.role3.stopKey(), index.role4.stopKey(),
				cardinality,
				true
			);
		} else {
			return index.scanWithConstraint(ctx, new ValueConstraint(ValueType.LITERAL));
		}
	}

	private Scan scanPredicateLiterals(IRI predicate) {
		RDFPredicate pred = rdfFactory.createPredicate(predicate);
		StatementIndex<SPOC.P,SPOC.O,SPOC.S,SPOC.C> index = pos;
		int typeSaltSize = rdfFactory.typeSaltSize;
		if (typeSaltSize == 1) {
			ByteSequence predb = new ByteArray(index.role1.keyHash(pred.getId()));
			int cardinality = VAR_CARDINALITY*VAR_CARDINALITY*VAR_CARDINALITY;
			return index.scan(
				predb, rdfFactory.writeSaltAndType(0, ValueType.LITERAL, null, index.role2.startKey()), index.role3.startKey(), index.role4.startKey(),
				predb, rdfFactory.writeSaltAndType(0, ValueType.LITERAL, null, index.role2.stopKey()), index.role3.stopKey(), index.role4.stopKey(),
				cardinality,
				true
			);
		} else {
			return index.scanWithConstraint(pred, new ValueConstraint(ValueType.LITERAL));
		}
	}

	private Scan scanPredicateGraphLiterals(IRI predicate, Resource graph) {
		RDFPredicate pred = rdfFactory.createPredicate(predicate);
		RDFContext ctx = rdfFactory.createContext(graph);
		StatementIndex<SPOC.C,SPOC.P,SPOC.O,SPOC.S> index = cpos;
		int typeSaltSize = rdfFactory.typeSaltSize;
		if (typeSaltSize == 1) {
			ByteSequence ctxb = new ByteArray(index.role1.keyHash(ctx.getId()));
			ByteSequence predb = new ByteArray(index.role2.keyHash(pred.getId()));
			int cardinality = VAR_CARDINALITY*VAR_CARDINALITY;
			return index.scan(
				ctxb, predb, rdfFactory.writeSaltAndType(0, ValueType.LITERAL, null, index.role3.startKey()), index.role4.startKey(),
				ctxb, predb, rdfFactory.writeSaltAndType(0, ValueType.LITERAL, null, index.role3.stopKey()), index.role4.stopKey(),
				cardinality,
				true
			);
		} else {
			return index.scanWithConstraint(ctx, pred, new ValueConstraint(ValueType.LITERAL));
		}
	}

	/**
	 * Performs a scan using any suitable index.
	 */
	public Scan scan(@Nonnull RDFIdentifier s, @Nonnull RDFIdentifier p, @Nonnull RDFIdentifier o, @Nullable RDFIdentifier c) {
		Scan scan;
		if (c == null) {
			int h = Math.floorMod(Objects.hash(s, p, o), 3);
			switch (h) {
				case 0:
					scan = spo.scan(s, p, o);
					break;
				case 1:
					scan = pos.scan(p, o, s);
					break;
				case 2:
					scan = osp.scan(o, s, p);
					break;
				default:
					throw new AssertionError();
			}
		} else {
			int h = Math.floorMod(Objects.hash(s, p, o, c), 3);
			switch (h) {
				case 0:
					scan = cspo.scan(c, s, p, o);
					break;
				case 1:
					scan = cpos.scan(c, p, o, s);
					break;
				case 2:
					scan = cosp.scan(c, o, s, p);
					break;
				default:
					throw new AssertionError();
			}
			scan = HalyardTableUtils.scanSingle(scan);
		}
		return scan;
	}

	public ValueIO.Reader createTableReader(ValueFactory vf, KeyspaceConnection conn) {
		return rdfFactory.valueIO.createStreamReader(vf);
	}
}
