package it.unitn.bigdataprojet.hdfsoperations;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Exist {
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

		Path scrDir = new Path("/user/cloudera/Files/generated/");

		if (exist(scrDir))
			System.exit(0);
		else
			System.exit(1);

	}

	public static boolean exist(Path srcDir) {
		try {
			fs = FileSystem.get(conf);

			return fs.exists(srcDir);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

}
