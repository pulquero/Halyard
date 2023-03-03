package com.msd.gin.halyard.common;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.security.KeyStore;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.apache.hadoop.conf.Configuration;

public final class SSLSettings {
	/** type for all key stores */
	private String keyStoreType;
	public URL keyStoreLocation;
	public char[] keyStorePassword;
	public URL trustStoreLocation;
	public char[] trustStorePassword;
	public String sslProtocol = "TLS";

	public static SSLSettings from(Configuration conf) throws MalformedURLException {
		String keyLoc = conf.get("es.net.ssl.keystore.location");
		String keyPass = conf.get("es.net.ssl.keystore.pass");
		String trustLoc = conf.get("es.net.ssl.truststore.location");
		String trustPass = conf.get("es.net.ssl.truststore.pass");
		SSLSettings sslSettings = new SSLSettings();
		sslSettings.keyStoreType = conf.get("es.net.ssl.keystore.type");
		sslSettings.keyStoreLocation = (keyLoc != null && !keyLoc.isEmpty()) ? new URL(keyLoc) : null;
		sslSettings.keyStorePassword = (keyPass != null && !keyPass.isEmpty()) ? keyPass.toCharArray() : null;
		sslSettings.trustStoreLocation = (trustLoc != null && !trustLoc.isEmpty()) ? new URL(trustLoc) : null;
		sslSettings.trustStorePassword = (trustPass != null && !trustPass.isEmpty()) ? trustPass.toCharArray() : null;
		sslSettings.sslProtocol = conf.get("es.net.ssl.protocol", "TLS");
		return sslSettings;
	}

	private static boolean isPKCS12(String loc) {
		return loc != null && (loc.endsWith(".p12") || loc.endsWith(".pfx") || loc.endsWith(".pkcs12"));
	}

	public SSLContext createSSLContext() throws IOException, GeneralSecurityException {
		KeyManager[] keyManagers = null;
		if (keyStoreLocation != null) {
			KeyStore keyStore = KeyStore.getInstance(getKeyStoreType(keyStoreLocation.getPath()));
			try (InputStream keyStoreIn = keyStoreLocation.openStream()) {
				keyStore.load(keyStoreIn, (keyStorePassword != null && keyStorePassword.length > 0) ? keyStorePassword : null);
			}
			KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
			keyManagerFactory.init(keyStore, keyStorePassword);
			keyManagers = keyManagerFactory.getKeyManagers();
		}
	
		TrustManager[] trustManagers = null;
		if (trustStoreLocation != null) {
			KeyStore trustStore = KeyStore.getInstance(getKeyStoreType(trustStoreLocation.getPath()));
			try (InputStream trustStoreIn = trustStoreLocation.openStream()) {
				trustStore.load(trustStoreIn, (trustStorePassword != null && trustStorePassword.length > 0) ? trustStorePassword : null);
			}
			TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
			trustManagerFactory.init(trustStore);
			trustManagers = trustManagerFactory.getTrustManagers();
		}
	
		SSLContext sslContext = SSLContext.getInstance(sslProtocol);
		sslContext.init(keyManagers, trustManagers, null);
		return sslContext;
	}

	private String getKeyStoreType(String location) {
		return (keyStoreType != null) ? keyStoreType : isPKCS12(location) ? "PKCS12" : "jks";
	}
}
