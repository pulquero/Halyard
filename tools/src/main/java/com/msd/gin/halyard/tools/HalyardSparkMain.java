package com.msd.gin.halyard.tools;

import com.msd.gin.halyard.util.Version;

import java.io.PrintWriter;
import java.util.Arrays;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.apache.hadoop.util.ToolRunner;

public final class HalyardSparkMain {
	HalyardSparkMain() {
	}

    public static void main(String args[]) throws Exception {
        String first = args.length > 0 ? args[0] : null;
        if ("-v".equals(first) || "--version".equals(first)) {
            System.out.println("halyardspark version " + Version.getVersionString());
        } else {
            AbstractHalyardTool tools[] = new AbstractHalyardTool[] {
                new HalyardSparkBulkLoad(),
            };
            for (AbstractHalyardTool tool : tools) {
                if (tool.name.equalsIgnoreCase(first)) {
                    int ret = ToolRunner.run(tool, Arrays.copyOfRange(args, 1, args.length));
                    if (ret != 0) throw new RuntimeException("Tool " + tool.name + " exits with code: " + ret);
                    return;
                }
            }
            try {
                if (first != null && !"-h".equals(first) && !"--help".equals(first)) {
                    String msg = "Unrecognized command or option: " + first;
                    System.out.println(msg);
                    throw new UnrecognizedOptionException(msg);
                }
            } finally {
                PrintWriter pw = new PrintWriter(System.out);
                HelpFormatter hf = new HelpFormatter();
                hf.printWrapped(pw, 100, "usage: sparkhalyard [ -h | -v | <command> [<genericHadoopOptions>] [-h] ...]");
                hf.printWrapped(pw, 100, "\ncommands are:\n----------------------------------------------------------------------------------------------------");
                int spaceCount = 17;
                char[] spaces = new char[spaceCount];
                Arrays.fill(spaces, ' ');
                for (AbstractHalyardTool tool : tools) {
                    hf.printWrapped(pw, 100, spaceCount, tool.name + new String(spaces, tool.name.length(), spaces.length-tool.name.length()) + tool.header);
                }
                hf.printWrapped(pw, 100, 0, "\ngenericHadoopOptions are:\n----------------------------------------------------------------------------------------------------");
                hf.printWrapped(pw, 100, 45, "-conf <configuration file>                   specify an application configuration file");
                pw.flush();
            }
        }
    }
}
