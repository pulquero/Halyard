package com.msd.gin.halyard.common;

import com.msd.gin.halyard.vocab.HALYARD;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Triple;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;

@ThreadSafe
public final class StatementIndices {
	private static final int PREFIXES = 3;
	private static final Statement[] EMPTY_STATEMENTS = new Statement[0];

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
		return HalyardTableUtils.scan(
			spo.concat(false, spo.role1.startKey(), spo.role2.startKey(), spo.role3.startKey(), spo.role4.startKey()),
			cosp.concat(true, cosp.role1.stopKey(), cosp.role2.stopKey(), cosp.role3.stopKey(), cosp.role4.stopKey()),
			maxCaching,
			true
		);
	}

	public Scan scanDefaultIndices() {
		return HalyardTableUtils.scan(
			spo.concat(false, spo.role1.startKey(), spo.role2.startKey(), spo.role3.startKey(), spo.role4.startKey()),
			osp.concat(true, osp.role1.stopKey(), osp.role2.stopKey(), osp.role3.stopKey(), osp.role4.stopKey()),
			maxCaching,
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
		int typeSaltSize = rdfFactory.idFormat.getSaltSize();
		if (typeSaltSize == 1) {
			return index.scan(
				rdfFactory.writeSaltAndType(0, ValueType.LITERAL, null, index.role1.startKey()), index.role2.startKey(), index.role3.startKey(), index.role4.startKey(),
				rdfFactory.writeSaltAndType(0, ValueType.LITERAL, null, index.role1.stopKey()), index.role2.stopKey(), index.role3.stopKey(), index.role4.stopKey(),
				maxCaching,
				true
			);
		} else {
			return index.scanWithConstraint(new ValueConstraint(ValueType.LITERAL));
		}
	}

	private Scan scanGraphLiterals(Resource graph) {
		RDFContext ctx = rdfFactory.createContext(graph);
		StatementIndex<SPOC.C,SPOC.O,SPOC.S,SPOC.P> index = cosp;
		int typeSaltSize = rdfFactory.idFormat.getSaltSize();
		if (typeSaltSize == 1) {
			ValueIdentifier.Format idFormat = rdfFactory.idFormat;
			ByteSequence ctxb = new ByteArray(index.role1.keyHash(ctx.getId(), idFormat));
			return index.scan(
				ctxb, rdfFactory.writeSaltAndType(0, ValueType.LITERAL, null, index.role2.startKey()), index.role3.startKey(), index.role4.startKey(),
				ctxb, rdfFactory.writeSaltAndType(0, ValueType.LITERAL, null, index.role2.stopKey()), index.role3.stopKey(), index.role4.stopKey(),
				maxCaching,
				true
			);
		} else {
			return index.scanWithConstraint(ctx, new ValueConstraint(ValueType.LITERAL));
		}
	}

	private Scan scanPredicateLiterals(IRI predicate) {
		RDFPredicate pred = rdfFactory.createPredicate(predicate);
		StatementIndex<SPOC.P,SPOC.O,SPOC.S,SPOC.C> index = pos;
		int typeSaltSize = rdfFactory.idFormat.getSaltSize();
		if (typeSaltSize == 1) {
			ValueIdentifier.Format idFormat = rdfFactory.idFormat;
			ByteSequence predb = new ByteArray(index.role1.keyHash(pred.getId(), idFormat));
			return index.scan(
				predb, rdfFactory.writeSaltAndType(0, ValueType.LITERAL, null, index.role2.startKey()), index.role3.startKey(), index.role4.startKey(),
				predb, rdfFactory.writeSaltAndType(0, ValueType.LITERAL, null, index.role2.stopKey()), index.role3.stopKey(), index.role4.stopKey(),
				maxCaching,
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
		int typeSaltSize = rdfFactory.idFormat.getSaltSize();
		if (typeSaltSize == 1) {
			ValueIdentifier.Format idFormat = rdfFactory.idFormat;
			ByteSequence ctxb = new ByteArray(index.role1.keyHash(ctx.getId(), idFormat));
			ByteSequence predb = new ByteArray(index.role2.keyHash(pred.getId(), idFormat));
			return index.scan(
				ctxb, predb, rdfFactory.writeSaltAndType(0, ValueType.LITERAL, null, index.role3.startKey()), index.role4.startKey(),
				ctxb, predb, rdfFactory.writeSaltAndType(0, ValueType.LITERAL, null, index.role3.stopKey()), index.role4.stopKey(),
				maxCaching,
				true
			);
		} else {
			return index.scanWithConstraint(ctx, pred, new ValueConstraint(ValueType.LITERAL));
		}
	}

	/**
	 * Performs a scan using any suitable index.
	 */
	Scan scanAny(@Nonnull RDFIdentifier<SPOC.S> s, @Nonnull RDFIdentifier<SPOC.P> p, @Nonnull RDFIdentifier<SPOC.O> o, @Nullable RDFIdentifier<SPOC.C> c) {
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
			scan = HalyardTableUtils.scanFirst(scan);
		}
		return scan;
	}

	/**
	 * Method constructing HBase Scan from a Statement pattern hashes, any of the arguments can be null
	 * @param subj optional subject Resource
	 * @param pred optional predicate IRI
	 * @param obj optional object Value
	 * @param ctx optional context Resource
	 * @return HBase Scan instance to retrieve all data potentially matching the Statement pattern
	 */
	public Scan scan(@Nullable RDFSubject subj, @Nullable RDFPredicate pred, @Nullable RDFObject obj, @Nullable RDFContext ctx) {
		if (ctx == null) {
			if (subj == null) {
				if (pred == null) {
					if (obj == null) {
						return spo.scan();
	                } else {
						return osp.scan(obj);
	                }
	            } else {
					if (obj == null) {
						return pos.scan(pred);
	                } else {
						return pos.scan(pred, obj);
	                }
	            }
	        } else {
				if (pred == null) {
					if (obj == null) {
						return spo.scan(subj);
	                } else {
						return osp.scan(obj, subj);
	                }
	            } else {
					if (obj == null) {
						return spo.scan(subj, pred);
	                } else {
						return scanAny(subj, pred, obj, null);
	                }
	            }
	        }
	    } else {
			if (subj == null) {
				if (pred == null) {
					if (obj == null) {
						return cspo.scan(ctx);
	                } else {
						return cosp.scan(ctx, obj);
	                }
	            } else {
					if (obj == null) {
						return cpos.scan(ctx, pred);
	                } else {
						return cpos.scan(ctx, pred, obj);
	                }
	            }
	        } else {
				if (pred == null) {
					if (obj == null) {
						return cspo.scan(ctx, subj);
	                } else {
						return cosp.scan(ctx, obj, subj);
	                }
	            } else {
					if (obj == null) {
						return cspo.scan(ctx, subj, pred);
	                } else {
						return scanAny(subj, pred, obj, ctx);
	                }
	            }
	        }
	    }
	}

    public Scan scanWithConstraints(RDFSubject subj, ValueConstraint subjConstraint, RDFPredicate pred, RDFObject obj, ValueConstraint objConstraint, RDFContext ctx) {
		if (subj == null && subjConstraint != null && (pred == null || objConstraint == null)) {
			return scanWithSubjectConstraint(subjConstraint, pred, obj, ctx);
		} else if (obj == null && objConstraint != null) {
			return scanWithObjectConstraint(subj, pred, objConstraint, ctx);
		} else {
			return scan(subj, pred, obj, ctx);
		}
	}

	private Scan scanWithSubjectConstraint(@Nonnull ValueConstraint subjConstraint, @Nullable RDFPredicate pred, @Nullable RDFObject obj, @Nullable RDFContext ctx) {
		if (ctx == null) {
			if (pred == null) {
				if (obj == null) {
					return spo.scanWithConstraint(subjConstraint);
                } else {
					return osp.scanWithConstraint(obj, subjConstraint);
                }
            } else {
				if (obj == null) {
					return pos.scanWithConstraint(pred, null, subjConstraint);
                } else {
					return pos.scanWithConstraint(pred, obj, subjConstraint);
                }
            }
        } else {
			if (pred == null) {
				if (obj == null) {
					return cspo.scanWithConstraint(ctx, subjConstraint);
                } else {
					return cosp.scanWithConstraint(ctx, obj, subjConstraint);
                }
            } else {
				if (obj == null) {
					return cpos.scanWithConstraint(ctx, pred, null, subjConstraint);
                } else {
					return cpos.scanWithConstraint(ctx, pred, obj, subjConstraint);
                }
            }
        }
    }

	private Scan scanWithObjectConstraint(@Nullable RDFSubject subj, @Nullable RDFPredicate pred, @Nonnull ValueConstraint objConstraint, @Nullable RDFContext ctx) {
		if (ctx == null) {
			if (subj == null) {
				if (pred == null) {
					return osp.scanWithConstraint(objConstraint);
                } else {
					return pos.scanWithConstraint(pred, objConstraint);
                }
            } else {
				if (pred == null) {
					return spo.scanWithConstraint(subj, null, objConstraint);
                } else {
					return spo.scanWithConstraint(subj, pred, objConstraint);
                }
            }
        } else {
			if (subj == null) {
				if (pred == null) {
					return cosp.scanWithConstraint(ctx, objConstraint);
                } else {
					return cpos.scanWithConstraint(ctx, pred, objConstraint);
                }
            } else {
				if (pred == null) {
					return cspo.scanWithConstraint(ctx, subj, null, objConstraint);
                } else {
					return cspo.scanWithConstraint(ctx, subj, pred, objConstraint);
                }
            }
        }
    }

	/**
	 * Parser method returning all Statements from a single HBase Scan Result
	 * 
	 * @param subj subject if known
	 * @param pred predicate if known
	 * @param obj object if known
	 * @param ctx context if known
	 * @param res HBase Scan Result
	 * @param vf ValueFactory
	 * @return array of Statements
	 */
	public Statement[] parseStatements(@Nullable RDFSubject subj, @Nullable RDFPredicate pred, @Nullable RDFObject obj, @Nullable RDFContext ctx, Result res, ValueFactory vf) {
		// multiple triples may have the same hash (i.e. row key)
		Statement[] stmts;
		if (!res.isEmpty()) {
			Cell[] cells = res.rawCells();
			if (cells.length == 1) {
				stmts = new Statement[] {parseStatement(subj, pred, obj, ctx, cells[0], vf)};
			} else {
				int cellCount = cells.length;
				stmts = new Statement[cellCount];
				for (int i=0; i<cellCount; i++) {
					stmts[i] = parseStatement(subj, pred, obj, ctx, cells[i], vf);
				}
			}
		} else {
			stmts = EMPTY_STATEMENTS;
		}
		return stmts;
	}

	/**
	 * Parser method returning Statement from a single HBase Result Cell
	 * 
	 * @param subj subject if known
	 * @param pred predicate if known
	 * @param obj object if known
	 * @param ctx context if known
	 * @param cell HBase Result Cell
	 * @param vf ValueFactory
	 * @return Statements
	 */
	public Statement parseStatement(@Nullable RDFSubject subj, @Nullable RDFPredicate pred, @Nullable RDFObject obj, @Nullable RDFContext ctx, Cell cell, ValueFactory vf) {
		ByteBuffer row = ByteBuffer.wrap(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
	    ByteBuffer cq = ByteBuffer.wrap(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
	    ByteBuffer cv = ByteBuffer.wrap(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
		StatementIndex<?,?,?,?> index = toIndex(row.get());
	    Statement stmt = index.parseStatement(subj, pred, obj, ctx, row, cq, cv, vf);
	    assert !row.hasRemaining();
	    assert !cq.hasRemaining();
	    assert !cv.hasRemaining();
		if (stmt instanceof Timestamped) {
			((Timestamped) stmt).setTimestamp(HalyardTableUtils.fromHalyardTimestamp(cell.getTimestamp()));
	    }
		return stmt;
	}


	public List<? extends KeyValue> insertKeyValues(Resource subj, IRI pred, Value obj, Resource context, long timestamp) {
		return toKeyValues(subj, pred, obj, context, false, timestamp, true);
	}
	public List<? extends KeyValue> deleteKeyValues(Resource subj, IRI pred, Value obj, Resource context, long timestamp) {
		return toKeyValues(subj, pred, obj, context, true, timestamp, false);
	}

	public List<? extends KeyValue> insertNonDefaultKeyValues(Resource subj, IRI pred, Value obj, Resource context, long timestamp) {
		return toKeyValues(subj, pred, obj, context, false, timestamp, false, true);
	}
	public List<? extends KeyValue> deleteNonDefaultKeyValues(Resource subj, IRI pred, Value obj, Resource context, long timestamp) {
		return toKeyValues(subj, pred, obj, context, true, timestamp, false, false);
	}

	/**
     * Conversion method from Subj, Pred, Obj and optional Context into an array of HBase keys
     * @param subj subject Resource
	 * @param pred predicate IRI
	 * @param obj object Value
	 * @param context optional context Resource
	 * @param delete boolean switch to produce KeyValues for deletion instead of for insertion
	 * @param timestamp long timestamp value for time-ordering purposes
	 * @param includeTriples boolean switch to include KeyValues for triples 
	 * @param rdfFactory RDFFactory
     * @return List of KeyValues
     */
	List<? extends KeyValue> toKeyValues(Resource subj, IRI pred, Value obj, Resource context, boolean delete, long timestamp, boolean includeTriples) {
		return toKeyValues(subj, pred, obj, context, delete, timestamp, true, includeTriples);
	}

	private List<? extends KeyValue> toKeyValues(Resource subj, IRI pred, Value obj, Resource context, boolean delete, long timestamp, boolean includeInDefaultGraph, boolean includeTriples) {
		List<KeyValue> kvs =  new ArrayList<KeyValue>(context == null ? PREFIXES : 2 * PREFIXES);
		KeyValue.Type type = delete ? KeyValue.Type.DeleteColumn : KeyValue.Type.Put;
		timestamp = HalyardTableUtils.toHalyardTimestamp(timestamp, !delete);
		appendKeyValues(subj, pred, obj, context, type, timestamp, includeInDefaultGraph, includeTriples, kvs);
		return kvs;
	}

	private void appendKeyValues(Resource subj, IRI pred, Value obj, Resource context, KeyValue.Type type, long timestamp, boolean includeInDefaultGraph, boolean includeTriples, List<KeyValue> kvs) {
		if(subj == null || pred == null || obj == null) {
			throw new NullPointerException();
		}
	
		RDFSubject sb = rdfFactory.createSubject(subj);
		RDFPredicate pb = rdfFactory.createPredicate(pred);
		RDFObject ob = rdfFactory.createObject(obj);
		RDFContext cb = rdfFactory.createContext(context);

		// generate HBase key value pairs from: row, family, qualifier, value. Permutations of SPO (and if needed CSPO) are all stored.
	    if (includeInDefaultGraph) {
			kvs.add(new KeyValue(spo.row(sb, pb, ob, cb), HalyardTableUtils.CF_NAME, spo.qualifier(sb, pb, ob, cb), timestamp, type, spo.value(sb, pb, ob, cb)));
			kvs.add(new KeyValue(pos.row(pb, ob, sb, cb), HalyardTableUtils.CF_NAME, pos.qualifier(pb, ob, sb, cb), timestamp, type, pos.value(pb, ob, sb, cb)));
			kvs.add(new KeyValue(osp.row(ob, sb, pb, cb), HalyardTableUtils.CF_NAME, osp.qualifier(ob, sb, pb, cb), timestamp, type, osp.value(ob, sb, pb, cb)));
	    }
	    if (context != null) {
	    	kvs.add(new KeyValue(cspo.row(cb, sb, pb, ob), HalyardTableUtils.CF_NAME, cspo.qualifier(cb, sb, pb, ob), timestamp, type, cspo.value(cb, sb, pb, ob)));
	    	kvs.add(new KeyValue(cpos.row(cb, pb, ob, sb), HalyardTableUtils.CF_NAME, cpos.qualifier(cb, pb, ob, sb), timestamp, type, cpos.value(cb, pb, ob, sb)));
	    	kvs.add(new KeyValue(cosp.row(cb, ob, sb, pb), HalyardTableUtils.CF_NAME, cosp.qualifier(cb, ob, sb, pb), timestamp, type, cosp.value(cb, ob, sb, pb)));
	    }
	
	    if (includeTriples) {
			if (subj.isTriple()) {
				Triple t = (Triple) subj;
				appendKeyValues(t.getSubject(), t.getPredicate(), t.getObject(), HALYARD.TRIPLE_GRAPH_CONTEXT, type, timestamp, false, true, kvs);
			}

			if (obj.isTriple()) {
				Triple t = (Triple) obj;
				appendKeyValues(t.getSubject(), t.getPredicate(), t.getObject(), HALYARD.TRIPLE_GRAPH_CONTEXT, type, timestamp, false, true, kvs);
			}
	    }
	}

	public boolean isTripleReferenced(KeyspaceConnection kc, Triple t) throws IOException {
		return hasSubject(kc, t)
			|| hasObject(kc, t)
			|| hasSubject(kc, t, HALYARD.TRIPLE_GRAPH_CONTEXT)
			|| hasObject(kc, t, HALYARD.TRIPLE_GRAPH_CONTEXT);
	}

	public boolean hasSubject(KeyspaceConnection kc, Resource subj) throws IOException {
		return HalyardTableUtils.exists(kc, spo.scan(rdfFactory.createSubject(subj)));
	}
	public boolean hasSubject(KeyspaceConnection kc, Resource subj, Resource ctx) throws IOException {
		return HalyardTableUtils.exists(kc, cspo.scan(rdfFactory.createContext(ctx), rdfFactory.createSubject(subj)));
	}

	public boolean hasObject(KeyspaceConnection kc, Value obj) throws IOException {
		return HalyardTableUtils.exists(kc, osp.scan(rdfFactory.createObject(obj)));
	}
	public boolean hasObject(KeyspaceConnection kc, Value obj, Resource ctx) throws IOException {
		return HalyardTableUtils.exists(kc, cosp.scan(rdfFactory.createContext(ctx), rdfFactory.createObject(obj)));
	}

	public Resource getSubject(KeyspaceConnection kc, ValueIdentifier id, ValueFactory vf) throws IOException {
		Scan scan = HalyardTableUtils.scanFirst(spo.scan(new RDFIdentifier<SPOC.S>(RDFRole.Name.SUBJECT, id)));
		try (ResultScanner scanner = kc.getScanner(scan)) {
			for (Result result : scanner) {
				if(!result.isEmpty()) {
					Cell[] cells = result.rawCells();
					Statement stmt = parseStatement(null, null, null, null, cells[0], vf);
					return stmt.getSubject();
				}
			}
		}
		return null;
	}

	public IRI getPredicate(KeyspaceConnection kc, ValueIdentifier id, ValueFactory vf) throws IOException {
		Scan scan = HalyardTableUtils.scanFirst(pos.scan(new RDFIdentifier<SPOC.P>(RDFRole.Name.PREDICATE, id)));
		try (ResultScanner scanner = kc.getScanner(scan)) {
			for (Result result : scanner) {
				if(!result.isEmpty()) {
					Cell[] cells = result.rawCells();
					Statement stmt = parseStatement(null, null, null, null, cells[0], vf);
					return stmt.getPredicate();
				}
			}
		}
		return null;
	}

	public Value getObject(KeyspaceConnection kc, ValueIdentifier id, ValueFactory vf) throws IOException {
		Scan scan = HalyardTableUtils.scanFirst(osp.scan(new RDFIdentifier<SPOC.O>(RDFRole.Name.OBJECT, id)));
		try (ResultScanner scanner = kc.getScanner(scan)) {
			for (Result result : scanner) {
				if(!result.isEmpty()) {
					Cell[] cells = result.rawCells();
					Statement stmt = parseStatement(null, null, null, null, cells[0], vf);
					return stmt.getObject();
				}
			}
		}
		return null;
	}
}
