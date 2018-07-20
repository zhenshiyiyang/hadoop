package hive;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * Created by hadoop on 2018/5/18.
 */
public class HiveJdbcCli {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive2://master:10000/default";
    private static String user = "hive";
    private static String password = "hive";
    private static String sql = "";
    private static ResultSet res;
    private static Connection conn;
    private static Statement stmt;
    private static final Logger log = Logger.getLogger(HiveJdbcCli.class);

//    public static void main(String[] args) {
//        res = null;
//        conn = null;
//        stmt = null;
//        try {
//            conn = getConn();
//            stmt = conn.createStatement();
//
//            // 第一步:存在就先删除
//            //String tableName = dropTable(stmt);
//
//            // 第二步:不存在就创建
//            //createTable(stmt, tableName);
//
//            // 第三步:查看创建的表
//            //showTables(stmt, tableName);
//
//            // 执行describe table操作
//            //describeTables(stmt, tableName);
//
//            // 执行load data into table操作
//            //loadData(stmt, tableName);
//
//            // 执行 select * query 操作
//            hiveDate(stmt, "t4");
//
//            // 执行 regular hive query 统计操作
//            //countData(stmt, tableName);
//
//        } catch (ClassNotFoundException e) {
//            e.printStackTrace();
//            log.error(driverName + " not found!", e);
//            System.exit(1);
//        } catch (SQLException e) {
//            e.printStackTrace();
//            log.error("Connection error!", e);
//            System.exit(1);
//        }
//    }

    private static void countData(Statement stmt, String tableName)
            throws SQLException {
        sql = "select count(1) from " + tableName;
        System.out.println("Running:" + sql);
        res = stmt.executeQuery(sql);
        System.out.println("执行“regular hive query”运行结果:");
        while (res.next()) {
            System.out.println("count ------>" + res.getString(1));
        }
        close();
    }

    public List<data> hiveDate(String tableName) throws ClassNotFoundException,SQLException {
        List<data> list = new ArrayList<data>();
        res = null;
        conn = null;
        stmt = null;
        Class.forName(driverName);
        conn = DriverManager.getConnection(url, user, password);
        stmt = conn.createStatement();
        sql = "select * from " + tableName;
        System.out.println("Running:" + sql);
        res = stmt.executeQuery(sql);
        ResultSet resultSet = res;
        System.out.println("执行 select * query 运行结果:");
        while (res.next()) {
            data d = new data();
            d.setId(res.getInt(1));
            d.setName(res.getString(2));
            d.setSalary(res.getInt(3));
            list.add(d);
            System.out.println(res.getInt(1) + "\t" + res.getString(2) +"\t"+res.getInt(3));
        }
        close();
        return list;
    }
    //为hbase查询hive结果数据返回
    private static void selectData(Statement stmt, String tableName)
            throws SQLException {
        sql = "select * from " + tableName;
        System.out.println("Running:" + sql);
        res = stmt.executeQuery(sql);
        System.out.println("执行 select * query 运行结果:");
        while (res.next()) {
            System.out.println(res.getInt(1) + "\t" + res.getString(2) +"\t"+res.getInt(3));

        }
        close();
    }
    private static void loadData(Statement stmt, String tableName)
            throws SQLException {
        String filepath = "/home/hadoop01/data";
        sql = "load data local inpath '" + filepath + "' into table "
                + tableName;
        System.out.println("Running:" + sql);
        res = stmt.executeQuery(sql);
        close();
    }

    private static void describeTables(Statement stmt, String tableName)
            throws SQLException {
        sql = "describe " + tableName;
        System.out.println("Running:" + sql);
        res = stmt.executeQuery(sql);
        System.out.println("执行 describe table 运行结果:");
        while (res.next()) {
            System.out.println(res.getString(1) + "\t" + res.getString(2));
        }
        close();
    }

    private static void showTables(Statement stmt, String tableName)
            throws SQLException {
        sql = "show tables '" + tableName + "'";
        System.out.println("Running:" + sql);
        res = stmt.executeQuery(sql);
        System.out.println("执行 show tables 运行结果:");
        if (res.next()) {
            System.out.println(res.getString(1));
        }
        close();
    }

    private static void createTable(Statement stmt, String tableName)
            throws SQLException {
        sql = "create table "
                + tableName
                + " (key int, value string)  row format delimited fields terminated by '\t'";
        stmt.executeQuery(sql);
        close();
    }

    private static String dropTable(Statement stmt) throws SQLException {
        // 创建的表名
        String tableName = "t4";
        sql = "drop table " + tableName;
        stmt.executeQuery(sql);
        close();
        return tableName;
    }

    private static Connection getConn() throws ClassNotFoundException,
            SQLException {
        Class.forName(driverName);
        Connection conn = DriverManager.getConnection(url, user, password);
        return conn;
    }
    //关闭连接
    public static void close(){
        try {
            if (res != null) {
                res.close();
                res = null;
            }
            if (conn != null) {
                conn.close();
                conn = null;
            }
            if (stmt != null) {
                stmt.close();
                stmt = null;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
