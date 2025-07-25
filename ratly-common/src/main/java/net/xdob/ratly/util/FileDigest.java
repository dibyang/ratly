package net.xdob.ratly.util;

import org.bouncycastle.crypto.Digest;

import java.io.File;
import java.io.IOException;
import java.util.regex.Matcher;

public interface FileDigest {
	Matcher getMatcher(String digest);
	void verifySavedDigest(File dataFile, Digest expectedDigest) throws IOException;
	Digest readStoredDigestForFile(File dataFile) throws IOException;
	Digest computeDigestForFile(File dataFile) throws IOException;
	Digest computeAndSaveDigestForFile(File dataFile);
	void saveDigestFile(File dataFile, Digest digest);
	void saveDigestFile(File dataFile, String digestString);
	File getDigestFileForFile(File file);
}
