package com.msd.gin.halyard.tools;

import com.msd.gin.halyard.sail.HBaseSail;
import com.msd.gin.halyard.strategy.HalyardEvaluationStrategy;

import java.io.FileOutputStream;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;

public class FunctionDoc {
	public static void main(String[] args) throws Exception {
		ValueFactory vf = SimpleValueFactory.getInstance();
		Model model = HalyardEvaluationStrategy.loadFunctionGraph(HBaseSail.getDefaultFunctionRegistry(), HBaseSail.getDefaultAggregateFunctionRegistry(), vf);
		try (FileOutputStream out = new FileOutputStream("functions.rdf")) {
			RDFWriter writer = Rio.createWriter(RDFFormat.RDFXML, out);
			writer.startRDF();
			for (Namespace ns : model.getNamespaces()) {
				writer.handleNamespace(ns.getPrefix(), ns.getName());
			}
			for (Statement stmt : model) {
				writer.handleStatement(stmt);
			}
			writer.endRDF();
		}
	}
}
