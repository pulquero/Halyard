package com.msd.gin.halyard.tools;

import com.msd.gin.halyard.tools.RDFLoadStager.FileExtension;

import org.junit.Test;

import static org.junit.Assert.*;

public class RDFLoadStagerTest {
	@Test
	public void fileExtension_type_gz() throws Exception {
		String filename = "data.ttl.gz";
		FileExtension ext = FileExtension.getExtension(filename);
		assertEquals(".ttl", ext.type);
		assertEquals(".gz", ext.compression);
	}

	@Test
	public void fileExtension_noType_gz() throws Exception {
		String filename = "README.gz";
		FileExtension ext = FileExtension.getExtension(filename);
		assertNull(ext.type);
		assertEquals(".gz", ext.compression);
	}

	@Test
	public void fileExtension_dotFile() throws Exception {
		String filename = ".foo.txt";
		FileExtension ext = FileExtension.getExtension(filename);
		assertEquals(".txt", ext.type);
		assertNull(ext.compression);
	}
}
