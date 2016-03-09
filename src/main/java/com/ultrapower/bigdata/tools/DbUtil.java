package com.ultrapower.bigdata.tools;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.log4j.Logger;


public class DbUtil {

	public static final Logger log = Logger.getLogger(DbUtil.class);

	// 重试次数
	public static final int NUM_RETRIES = 3600;

	// 休眠时间 10 s
	public static final int SLEEP_TIME = 1000;
	
	//
	public static int thredCount  = 0;

	/**
	 * 
	 */

	public static String dataPath = "";
	public static String dbName = "";
	public static String moduleName = "";
	/**
	 * 
	 * @param dbType
	 *            hive or oracle
	 * @return
	 * @return
	 */
	private static Properties props;
	static {
		props = new Properties();
		try {
			InputStream is = new BufferedInputStream(new FileInputStream("../conf/table.properties"));
			props.load(is);
			dataPath = props.getProperty("dataPath");
			thredCount = null != props.getProperty("thredcount") ? Integer.parseInt(props.getProperty("thredcount")) : 10 ;
			dbName = props.getProperty("dbName");
			moduleName = props.getProperty("moduleName");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 多次重试连接oracle
	 * 
	 * @return
	 */
	public static Connection getRetriesConnection() {
		Connection con = null;
		for (int tries = 1; tries <= NUM_RETRIES; tries++) {
			con = getCon("oracle");
			if (null != con) {
				break;
			}
			log.info("\n重试了  [" + tries + "] 次连接oracle");
			try {
				Thread.sleep(SLEEP_TIME);
			} 
			catch (InterruptedException e) {
				// Do this conversion rather than let it out because do not want
				// to
				// change the method signature.
				Thread.currentThread().interrupt();

				// throw new IOException("Interrupted", e);
			}
		}
		return con;
	}

	public static void closeConnection(Connection con) {
		if (null != con) {
			try {
				con.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				// e.printStackTrace();
			}
		}

	}

	public static void closeStatement(Statement sta) {
		if (null != sta) {
			try {
				sta.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				// e.printStackTrace();
			}
		}

	}

	public static Connection getCon(String dbType) {
		Connection con = null;
		try {
			// hive jdbc url

			if (dbType.equals("hive")) {
				int hiveVersion = Integer
						.parseInt(props.getProperty("connect"));
				String driverName = props.getProperty("driverName"
						+ hiveVersion);
				String url = props.getProperty("url" + hiveVersion);
				Class.forName(driverName);
				con = DriverManager.getConnection(url, "", "");
			} else if (dbType.equals("oracle")) {
				String url = props.getProperty("oracleUrl");
				String driverName = props.getProperty("oracleDriver");
				String username = props.getProperty("username");
				String passwd = props.getProperty("passwd");
				Class.forName(driverName);
				con = DriverManager.getConnection(url, username, passwd);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return con;
	}

	/**
	 * 执行HIVE sql update操作
	 * 
	 * @param sql
	 */
	public static boolean executeUpdate(String sql, String name) {
		Connection hiveCon = null;
		Statement stat = null;
		boolean code = true;
		try {
			log.info("创建数据库连接开始.........");
			hiveCon = DbUtil.getCon("hive");
			stat = hiveCon.createStatement();
			log.info("创建数据库连接结束.........");
			
			
			log.info("切换数据库到dns开始.........");
			//stat.executeQuery("use " + dbName);
			log.info("切换数据库到dns结束.........");
			
			log.info("设置任务名" + name + "开始.........");
			stat.executeQuery(name);
			log.info("设置任务名" + name + "结束.........");
			
			log.info("任务执行开始");
			stat.executeQuery(sql);
			log.info("任务执行结束");
			
		} catch (Exception e) {
			code = false;
			log.error(sql);
			e.printStackTrace();
		} finally {
			closeConnection(hiveCon);
			closeStatement(stat);
		}
		return code;
	}

	public static void main(String[] args) {
		getRetriesConnection();
		System.out.println(".....");
	}
}
