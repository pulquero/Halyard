/*
 * Copyright 2016 Merck Sharp & Dohme Corp. a subsidiary of Merck & Co.,
 * Inc., Kenilworth, NJ, USA.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.msd.gin.halyard.sail;

import com.msd.gin.halyard.common.HBaseServerTestInstance;
import com.msd.gin.halyard.common.Hashes;
import com.msd.gin.halyard.vocab.HALYARD;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.IOUtils;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class LiteralSearchStatementScannerTest implements Runnable {

    static HBaseSail hbaseSail;
    static ServerSocket server;
    static String response;


    @BeforeClass
    public static void setup() throws Exception {
        server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        hbaseSail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "testLiteralSearch", true, 0, true, 0, "http://" + InetAddress.getLoopbackAddress().getHostAddress() + ":" + server.getLocalPort(), null);
        hbaseSail.initialize();
    }

    @AfterClass
    public static void teardown() throws Exception {
        hbaseSail.shutDown();
        server.close();
    }

    @Test
    public void statementLiteralSearchTest() throws Exception {
        Literal val = SimpleValueFactory.getInstance().createLiteral("Whatever Text");
		response = "HTTP/1.1 200 OK\ncontent-type: application/json; charset=UTF-8\ncontent-length: 30\n\r\n{\"hits\":{\"hits\":[{\"_id\":\"" + Hashes.id(val) + "\",\"_source\":{\"label\":\"" + val.getLabel() + "\",\"datatype\":\""
				+ val.getDatatype() + "\"}}]}}";
        Thread t = new Thread(this);
        t.setDaemon(true);
        t.start();
        IRI whatever = SimpleValueFactory.getInstance().createIRI("http://whatever");
		try (SailConnection conn = hbaseSail.getConnection()) {
			conn.addStatement(whatever, whatever, val);
			try (CloseableIteration<? extends Statement, SailException> iter = conn.getStatements(null, null, SimpleValueFactory.getInstance().createLiteral("what", HALYARD.SEARCH_TYPE), true)) {
				assertTrue(iter.hasNext());
			}
		}
    }

    @Override
    public void run() {
        try (Socket s = server.accept()) {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()))) {
                String line;
                int length = 0;
                while ((line = in.readLine()) != null && line.length() > 0) {
                    if (line.startsWith("Content-Length:")) {
                        length = Integer.parseInt(line.substring(15).trim());
                    }
                    System.out.println(line);
                }
                char b[] = new char[length];
                in.read(b);
                System.out.println(b);
                try (OutputStream out = s.getOutputStream()) {
                    IOUtils.write(response, out, StandardCharsets.UTF_8);
                }
            }
        } catch (IOException ex) {
			LoggerFactory.getLogger(LiteralSearchStatementScannerTest.class).error("Error reading from socket", ex);
        }
    }
}
