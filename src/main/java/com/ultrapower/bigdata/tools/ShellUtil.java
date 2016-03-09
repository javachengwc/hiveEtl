package com.ultrapower.bigdata.tools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class ShellUtil {
	
	// 保存进程的输入流信息
	private List<String> stdoutList = new ArrayList<String>();
	// 保存进程的错误流信息
	private List<String> erroroutList = new ArrayList<String>();
	
	public static final String HADOOP_HOME="/home/cup/hadoop-2.3.0-cdh5.0.0/bin/";
	    
	
	public   int executeCommand(String command) throws IOException, InterruptedException {
		System.out.println(command);
		int i = -2;
		// 先清空
		this.stdoutList.clear();
		this.erroroutList.clear();
		Process p = null;
		p = Runtime.getRuntime().exec(command);
		// 创建2个线程，分别读取输入流缓冲区和错误流缓冲区
		ThredTools stdoutUtil = new ThredTools(p.getInputStream(),
				this.stdoutList);
		ThredTools erroroutUtil = new ThredTools(p.getErrorStream(),
				this.erroroutList);
		// 启动线程读取缓冲区数据
		stdoutUtil.start();
		erroroutUtil.start();
		i = p.waitFor();
		return i;
	}

}
