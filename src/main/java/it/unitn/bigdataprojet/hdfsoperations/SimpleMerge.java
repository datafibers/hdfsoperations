package it.unitn.bigdataprojet.hdfsoperations;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class SimpleMerge {
	private static Configuration conf = null;
	private static FileSystem fs = null;

	public static void main(String[] args) {
		
		conf = new Configuration();

		conf.addResource("/etc/hadoop/conf/core-site.xml");
		conf.addResource("/etc/hadoop/conf/hdfs-site.xml");
		conf.set("fs.defaultFS", "hdfs://quickstart.cloudera:8020/");
		//conf.set("fs.defaultFS", "hdfs://127.0.0.1:8020/");
		conf.set("hadoop.job.ugi", "cloudera");
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());

		Path scrDir = new Path("/user/cloudera/streaming/merge/");
		
		Long time = System.currentTimeMillis();
		Path copyTo = new Path("/user/cloudera/streaming/store/data_"+time+".txt");
		Path scrDes = new Path("/user/cloudera/workpig/data.txt");
		boolean result = merge(scrDir, scrDes, null,copyTo);

		if (result)
			System.exit(0);
		else
			System.exit(1);

	}

	public static boolean merge(Path srcDir, Path dstFile, String addString,Path copyTo) {
		try {
			fs = FileSystem.get(conf);
			if (fs.isFile(dstFile)) {		
				fs.rename(dstFile, copyTo);
			}

			return FileUtilPlus.copyMerge_Recursive(fs, srcDir, fs, dstFile, true, conf, addString);


		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

}
