package it.unitn.bigdataprojet.hdfsoperations;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class FileUtilPlus {

	/**
	 * based on org.apache.hadoop.fs.FileUtil.copyMerge(..), this implementation
	 * allow get all files recursively
	 * 
	 * @param srcFS
	 * @param srcDir
	 * @param dstFS
	 * @param dstFile
	 * @param deleteSource
	 * @param conf
	 * @param addString
	 * @return
	 * @throws IOException
	 */
	public static boolean copyMerge_Recursive(FileSystem srcFS, Path srcDir, FileSystem dstFS, Path dstFile, boolean deleteSource,
			Configuration conf, String addString) throws IOException {
		dstFile = checkDest(srcDir.getName(), dstFS, dstFile, false);

		if (!srcFS.getFileStatus(srcDir).isDirectory())
			return false;

		OutputStream out = dstFS.create(dstFile);

		try {
			ArrayList<FileStatus> contents = getallfile(srcDir, srcFS);

			for (FileStatus content : contents) {

				InputStream in = srcFS.open(content.getPath());

				try {
					IOUtils.copyBytes(in, out, conf, false);
					if (addString != null)
						out.write(addString.getBytes("UTF-8"));

				} finally {
					in.close();
				}

			}
		} finally {
			out.close();
		}
		
		
		if (deleteSource) {
			return srcFS.delete(srcDir, true);	
		} else {
			return true;
		}
	}

	public static ArrayList<FileStatus> getallfile(Path srcDir, FileSystem srcFS) {
		ArrayList<FileStatus> result = new ArrayList<FileStatus>();
		try {
			FileStatus contentStart[] = srcFS.listStatus(srcDir);
			if ((null != contentStart) && (contentStart.length > 0)) {
				for (FileStatus fstatus : contentStart) {
					if (fstatus.isFile()) {
						result.add(fstatus);
					} else {
						result.addAll(getallfile(fstatus.getPath(), srcFS));
					}
				}
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return result;

	}

	private static Path checkDest(String srcName, FileSystem dstFS, Path dst, boolean overwrite) throws IOException {
		if (dstFS.exists(dst)) {
			FileStatus sdst = dstFS.getFileStatus(dst);
			if (sdst.isDirectory()) {
				if (null == srcName) {
					throw new IOException("Target " + dst + " is a directory");
				}
				return checkDest(null, dstFS, new Path(dst, srcName), overwrite);
			} else if (!overwrite) {
				throw new IOException("Target " + dst + " already exists");
			}
		}
		return dst;
	}
}
