package com.ultrapower.bigdata.hive;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


import org.apache.log4j.Logger;

import com.ultrapower.bigdata.tools.DateUtil;
import com.ultrapower.bigdata.tools.DbUtil;
import com.ultrapower.bigdata.tools.ParseArgs;
import com.ultrapower.bigdata.tools.Utils;

public class JobScheduler {
	
	public static final Logger log = Logger.getLogger(JobScheduler.class);
	//空闲的线程数
	public  static final int  activeAlive = 0 ;

	/**
	 * 获取hdfs上2g高流量迁移目录 
	 * @param date
	 */
	public static  String getDataPath(String date){
		String dataPath = DbUtil.dataPath;
		String hdfsPath = dataPath.split(",")[2]+date;
		return hdfsPath+File.separator;
	}
	
	/**
	 * Hive数据汇聚
	 * @param map
	 */
	public static void aggreDataToHive(Map<String, String> map){
		List<String> hiveList = getSQL(0);
		//执行hive
		String sql = "";	
		for (String key : hiveList) {
			Long startTime = System.currentTimeMillis();
			String temp[]  = key.split("#");
			String jobName ="set mapred.job.name="+map.get("ptime")+"%"+temp[0];
			sql = Utils.parse(temp[1], map);
			
			log.info(sql);
			DbUtil.executeUpdate(sql,jobName);
			Long endTime = System.currentTimeMillis();
			log.info("日期 : "+map.get("ptime")+" 以上 hive sql 操作 耗时 : ["+(endTime-startTime)/1000 +"] s \n\n" );
	    }
		
	}
	
	/**
	 * hive导到oracle表中的操作
	 * @param map
	 */
	public static void loadDataToDB(Map<String, String> map){
		List<String> shellList = getSQL(1);
		//执行hive
		String sql = "";	
		//hive导到oracle表中的操作
		for (String key : shellList) {
			Long startTime = System.currentTimeMillis();
			String temp[]  = key.split("#");
			sql = Utils.parse(temp[1], map);
			log.info(sql);
			startExport(sql, temp[0],map.get("ptime"));
			Long endTime = System.currentTimeMillis();
			log.info("日期 :  "+map.get("ptime")+" 导入到oracle操作: " +key.replace("{time}", map.get("ptime"))+ " 耗时" + (endTime-startTime)/1000 +"s \n" );
	    }
	}
	
	/**
	 * hive 导到 oracle 操作
	 * @param arg
	 */
	private static void startExport(String hiveSql,String oracleTable,String date){
		log.info("start export data from hive to oracle :"+date);
		log.info("hive sql is :"+ hiveSql);
		log.info("oracle table is :"+oracleTable);
		Connection hiveCon = null, oracleCon = null;
		Statement hiveStmt = null, oracleStmt = null;
		PreparedStatement pst = null;
		int exceptionRow = 0;
		int columnCount  = 0;
		StringBuffer insertSql = new StringBuffer();
		//行数
		int rowCount = 0;
		Date dt = null;
		try {
			hiveCon =  DbUtil.getCon("hive");
			oracleCon = DbUtil.getCon("oracle");
			
			hiveStmt = hiveCon.createStatement();
			oracleStmt = oracleCon.createStatement();
			ResultSet hiveRes  = hiveStmt.executeQuery(hiveSql);
			columnCount = hiveRes.getMetaData().getColumnCount();
			//System.out.println("列的类型："+hiveRes.getMetaData().getColumnTypeName(7));
			log.info("delete from "
					+ oracleTable
					+ " where dt = "
					+ date
					+ " : "
					+ (oracleStmt.executeUpdate("delete from " + oracleTable + " where dt = to_date('" + date + "', 'yyyy-mm-dd')") == 1 ? "true" : "false"));
			insertSql.append("insert into "+oracleTable+" values(");
			for(int j = 1 ; j <= columnCount ; j ++ ){
				insertSql.append("?,");
			}
			insertSql.deleteCharAt(insertSql.length() - 1);
			insertSql.append(")");
			pst = oracleCon.prepareStatement(insertSql.toString());
			while(hiveRes.next()){
				rowCount ++ ;
				for(int j = 1 ; j <= columnCount ; j ++ ){
					String columnName = hiveRes.getMetaData().getColumnName(j);
					if("hour".equals(columnName))
							pst.setString(j, hiveRes.getString(j) != null ? hiveRes.getString(j) : null);
					if("dt".equals(columnName)) {
						dt = new Date(DateUtil.StringToDate(date, "yyyyMMdd").getTime());
						pst.setDate(j, dt);
					}else{
						pst.setString(j, hiveRes.getString(j) != null ? hiveRes.getString(j) : null);
					}
				}
				pst.addBatch();
				if(rowCount % 50000 == 0){
					pst.executeBatch();
					pst.clearBatch();
				}
			}
			pst.executeBatch();
		}
		catch(Exception e){
			e.printStackTrace();
		}
		finally{
			DbUtil.closeConnection(hiveCon);
			DbUtil.closeStatement(hiveStmt);
		}
		log.info("----- success load to oralce  "+(rowCount-exceptionRow)+"  row  ------- ");
	}
	
	
	/**
	 *  获取oracle表中存放的作业内容
	 * @param type 有jdbc 操作的 和  shell命令两种,0代表jdbc,1代表hive到oracle命令
	 */
	public static List<String> getSQL(int type){
		List<String> list = new ArrayList<String>();
		Connection oracleCon = null;
		Statement stat = null;
		try {
			oracleCon = DbUtil.getRetriesConnection();
			stat = oracleCon.createStatement();
//			String sql = "select name,dbms_lob.substr(info,4000) info from hive_sql_config where type =" + type + " and module_name = '" + DbUtil.moduleName + "' order by id ";
//			String sql = "select name,info from xdr_quality_config where type =" + type + " and module_name = '" + DbUtil.moduleName + "' order by id ";
			String sql = "select name,info from hive_sql_config where type =" + type + " and module_name = '" + DbUtil.moduleName + "' order by id ";
			ResultSet rs = stat.executeQuery(sql);
			while(rs.next()){
				//jdbc操作 获取name字段便于设置hive job的名字 set mapred.job.name
				list.add(rs.getString("name")+"#"+oracleClobToString(rs.getClob("info")));
//				list.add(rs.getString("name")+"#"+rs.getString("info"));
				System.out.println(oracleClobToString(rs.getClob("info")));
			}
		}	
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally{
			DbUtil.closeConnection(oracleCon);
			DbUtil.closeStatement(stat);
		}
		return list;
	}
	
	/**
	 * 将Clob对象转换为String对象
	 * @param clob
	 * @return
	 */
	public static String oracleClobToString(Clob clob){ 
        try {
			return (clob != null ? clob.getSubString(1, (int) clob.length()) : null);
		} catch (SQLException e) {
			log.info("ORACLE Clob类型转String转换错误");
			e.printStackTrace();
		} 
        return null;
    } 
	
	// 将字CLOB转成STRING类型
	public static String ClobToString(Clob clob){
		String reString = "";
		Reader is = null;
		try {
			is = clob.getCharacterStream();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}// 得到流
		BufferedReader br = new BufferedReader(is);
		String s = null;
		try {
			s = br.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
		StringBuffer sb = new StringBuffer();
		// 执行循环将字符串全部取出付值给StringBuffer由StringBuffer转成STRING
		try {
			while (s != null) {
				sb.append(s);
				s = br.readLine();
			}
			is.close();
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		reString = sb.toString();
		return reString;
	}

	
	public static void main(String[] args) {
		
		args = new String[2];
		
		args[0]="-ptime";
		args[1]="20150709";
		
		Long startTime = System.currentTimeMillis();
		ParseArgs parse = new ParseArgs(args);
		//hive数据汇聚
//		aggreDataToHive(parse.getMap());
		//hive导到oracle表中的操作
		loadDataToDB(parse.getMap());
		Long endTime = System.currentTimeMillis();
		log.info("Hive数据分析 >> 汇聚 >> 导入到 oracle 总耗时 ["+(endTime-startTime)/1000/60+"] 分钟");	
	}
}
