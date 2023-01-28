package com.msd.gin.halyard.tools;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class HadoopLogRule implements TestRule {
	public static HadoopLogRule create() {
		return new HadoopLogRule();
	}

	@Override
	public Statement apply(Statement base, Description description) {
		return new Statement() {
			Pattern errPattern = Pattern.compile("Exception|Error|Caused by: |Suppressed: ", Pattern.CASE_INSENSITIVE);
			Pattern exclPattern = Pattern.compile("Shuffle Errors|IO_ERROR|NOT_SERVING_REGION_EXCEPTION|error_?prone|rdf4j-common-exception", Pattern.CASE_INSENSITIVE);

			@Override
			public void evaluate() throws Throwable {
				try {
					base.evaluate();
				} catch(Throwable err) {
					Path clusterHome = Paths.get("target/test/data/testCluster");
					if (Files.exists(clusterHome)) {
						Files.walk(clusterHome)
						.filter(p -> "syslog".equals(p.getFileName().toString()))
						.forEach(f -> {
							try(Stream<String> lines = Files.lines(f)) {
								String log = lines.filter(l -> (errPattern.matcher(l).find() && !exclPattern.matcher(l).find()) || l.startsWith("\tat "))
		    					.map(l -> "*** "+l+"\n")
		    					.reduce("", String::concat);
								System.out.println(log);
			    			} catch(IOException ioe) {
			    				ioe.printStackTrace();
							}
						});
					} else {
						System.out.println("*** It appears the cluster failed to start");
					}
					throw err;
				}
			}
		};
	}
}
