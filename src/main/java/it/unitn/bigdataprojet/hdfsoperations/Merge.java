package it.unitn.bigdataprojet.hdfsoperations;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Merge {
	private static Configuration conf = null;
	private static FileSystem fs = null;

	public static void main(String[] args) {
		
		conf = new Configuration();

		conf.addResource("/etc/hadoop/conf/core-site.xml");
		conf.addResource("/etc/hadoop/conf/hdfs-site.xml");
		conf.set("fs.defaultFS", "hdfs://quickstart.cloudera:8020/");
		// conf.set("fs.defaultFS", "hdfs://127.0.0.1:8020/");
		conf.set("hadoop.job.ugi", "cloudera");
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());

		Path scrDir = new Path("/user/cloudera/streaming/result/");

		Long time = System.currentTimeMillis();
		Path scrDes = new Path("/user/cloudera/streaming/merge/merged"+time+".txt");
		boolean result = merge(scrDir, scrDes, null);

		if (result)
			System.exit(0);
		else
			System.exit(1);

	}

	public static boolean merge(Path srcDir, Path dstFile, String addString) {
		try {
			fs = FileSystem.get(conf);
			if (fs.isFile(dstFile)) {
				fs.delete(dstFile, true);
			}

			return FileUtilPlus.copyMerge_Recursive(fs, srcDir, fs, dstFile, true, conf, addString);

			

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

}
