package com.ultrapower.bigdata.hdfs;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hive.com.esotericsoftware.minlog.Log;
import org.apache.log4j.Logger;

import com.ultrapower.bigdata.hive.JobScheduler;

public class HdfsOp {
	
	public static final Logger log = Logger.getLogger(HdfsOp.class);

	public static Configuration conf = null;

	static {

		conf = new Configuration();

		//conf.addResource("../conf/core-site.xml");
		//conf.addResource("../conf/hdfs-site.xml");
		
		//conf.addResource("core-site.xml");
		//conf.addResource("hdfs-site.xml");

	}
	
	//创建hdfs目录，先判断是否存在
	public static void createDir(String dirName){
		FileSystem fs = null;
		try{
			fs = FileSystem.get(conf);
			Path path = new Path(dirName);
			if(fs.exists(path)){ 
				return;
			}
			fs.mkdirs(path);
			log.info("create hdfs path "+path);
			
		}
		catch(Exception e){
			log.error("create hdfs path "+dirName+" fail");
			e.printStackTrace();
		}
		
	}
	
	//上传本地文件
    public static boolean uploadFile(String src,String dst,String fileName){
    	  boolean code = true;
          Configuration conf = new Configuration();
          FileSystem fs;
          Path srcPath = new Path(src); //原路径
          Path dstPath = new Path(dst); //目标路径
		try {
			fs = FileSystem.get(conf);
			if(fs.exists(new Path(dstPath+"/"+fileName))){
				//log.warn(" hdfs file  "+dstPath+"/"+fileName +" is exsits ");
				return false;
			}
			fs.copyFromLocalFile(false,srcPath, dstPath);
		} catch (IOException e) {
			code = false;
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		return code;
    }

}
