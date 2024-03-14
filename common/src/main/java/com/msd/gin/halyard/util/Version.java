package com.msd.gin.halyard.util;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Enumeration;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

public final class Version {
	private Version() {}

	public static String getVersionString() {
		ClassLoader cl = Thread.currentThread().getContextClassLoader();
		try {
	    	for (Enumeration<URL> iter = cl.getResources("META-INF/MANIFEST.MF"); iter.hasMoreElements(); ) {
	    		URL loc = iter.nextElement();
	    		try (InputStream in = loc.openStream()) {
	    			Manifest manifest = new Manifest(in);
	    			String vendor = manifest.getMainAttributes().getValue(Attributes.Name.IMPLEMENTATION_VENDOR);
	    			if ("halyard".equals(vendor)) {
		    			String version = manifest.getMainAttributes().getValue(Attributes.Name.IMPLEMENTATION_VERSION);
		    			String build = manifest.getMainAttributes().getValue("Implementation-Build");
		    	        return version + " (" + build + ")";
	    			}
	    		}
	    	}
	    	return "unknown";
		} catch (IOException ioe) {
			ioe.printStackTrace();
			return "error retrieving version";
		}
    }
}
