package Index.dataprocess;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
/*
	Created by LHM on 2017.04.17
*/
public class HbaseTest {
    public static Configuration conf;
    public static Connection connection;
    public static Admin admin;

    //初始化连接
    public static void init(){
        File workaround = new File(".");
        System.getProperties().put("hadoop.home.dir", workaround.getAbsolutePath());
        new File("./bin").mkdirs();
        try {
            new File("./bin/winutils.exe").createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        Configuration conf=HBaseConfiguration.create();
        try {
            connection=ConnectionFactory.createConnection(conf);
            admin=connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    //关闭连接
    public static void close(){
        try{
            if(admin!=null)
                admin.close();
            if(connection!=null)
                connection.close();
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    //表是否存在
    public static boolean tableIsExist(String tableName) throws IOException{
        TableName tableName1 = TableName.valueOf(tableName);
        if(admin.tableExists(tableName1))
            return true;
        else
            return false;
    }
    //建表
    public static void createTable(String tableName,String columnFamily) throws IOException{
        TableName tableName1 = TableName.valueOf(tableName);
        if(admin.tableExists(tableName1)){
            System.out.println("Table exists!");
        }else{
            HTableDescriptor table=new HTableDescriptor(tableName1);
            table.addFamily(new HColumnDescriptor(columnFamily));
            admin.createTable(table);
            System.out.println("create table success!");
        }
    }
    //删表
    public static void deleteTable(String tableName) throws IOException{
        TableName tableName1=TableName.valueOf(tableName);
        if(admin.tableExists(tableName1)){
            admin.disableTable(tableName1);
            admin.deleteTable(tableName1);
            System.out.println("delete table success!");
        }
    }
    //清空表中数据
    public static void clearTable(String tableName) throws IOException{
        HColumnDescriptor[] columnFamilies = null;
        if(tableIsExist(tableName)){
            TableName tableName1 = TableName.valueOf(tableName);
            HTableDescriptor table=admin.getTableDescriptor(tableName1);
            columnFamilies=table.getColumnFamilies();
            deleteTable(tableName);
            for(HColumnDescriptor cf:columnFamilies){
                createTable(tableName, cf.getNameAsString());
            }
        }


    }
    //查看已有表
    public static void listTables() throws IOException{
        HTableDescriptor[] tables=admin.listTables();
        System.out.println("tables list：");
        for(HTableDescriptor table:tables){
            System.out.println(table.getNameAsString());
        }
    }

    //插入数据
    public static void insterRow(String tableName,String rowkey,String colFamily,String column,String value) throws IOException{
        Table table=connection.getTable(TableName.valueOf(tableName));
        if(tableIsExist(tableName)){
            Put put=new Put(Bytes.toBytes(rowkey));
            put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(column), Bytes.toBytes(value));
            table.put(put);
            table.close();
            System.out.println("insert data success！");
        }else{
            System.out.println("table is not exist!");
        }
    }
    //删除数据
    public static void deleRow(String tableName,String rowkey) throws IOException{
        Table table=connection.getTable(TableName.valueOf(tableName));
        if(tableIsExist(tableName)){
            Delete delete=new Delete(Bytes.toBytes(rowkey));
            table.delete(delete);
            table.close();
            System.out.println("delete data success！");
        }else{
            System.out.println("table is not exist!");
        }
    }
    //删除数据的指定列族
    public static void deleRow(String tableName,String rowkey,String colFamily) throws IOException{
        Table table=connection.getTable(TableName.valueOf(tableName));
        if(tableIsExist(tableName)){
            Delete delete=new Delete(Bytes.toBytes(rowkey));
            delete.addFamily(Bytes.toBytes(colFamily));
            table.delete(delete);
            table.close();
            System.out.println("delete data success！");
        }else{
            System.out.println("table is not exist!");
        }
    }
    //删除数据的指定列
    public static void deleRow(String tableName,String rowkey,String colFamily,String column) throws IOException{
        Table table=connection.getTable(TableName.valueOf(tableName));
        if(tableIsExist(tableName)){
            Delete delete=new Delete(Bytes.toBytes(rowkey));
            delete.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(column));
            table.delete(delete);
            table.close();
            System.out.println("delete data success！");
        }else{
            System.out.println("table is not exist!");
        }
    }
    //根据Rowkey查找数据
    public static void getRow(String tableName,String rowkey) throws IOException{
        Table table=connection.getTable(TableName.valueOf(tableName));
        if(tableIsExist(tableName)){
            Get get=new Get(Bytes.toBytes(rowkey));
            Result result=table.get(get);
            printFormat(result);
        }else{
            System.out.println("table is not exist!");
        }
    }
    //根据Rowkey获取指定列族的数据
    public static void getRow(String tableName,String rowkey,String colFamily) throws IOException{
        Table table=connection.getTable(TableName.valueOf(tableName));
        if(tableIsExist(tableName)){
            Get get=new Get(Bytes.toBytes(rowkey));
            get.addFamily(Bytes.toBytes(colFamily));
            Result result=table.get(get);
            printFormat(result);
        }else{
            System.out.println("table is not exist!");
        }
    }
    //根据Rowkey获取指定列的数据
    public static void getRow(String tableName,String rowkey,String colFamily,String column) throws IOException{
        Table table=connection.getTable(TableName.valueOf(tableName));
        if(tableIsExist(tableName)){
            Get get=new Get(Bytes.toBytes(rowkey));
            get.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(column));
            Result result=table.get(get);
            printFormat(result);
        }else{
            System.out.println("table is not exist!");
        }
    }
    //根据表名查找数据
    public static void getAllRow(String tableName) throws IOException{
        Table table=connection.getTable(TableName.valueOf(tableName));
        if(tableIsExist(tableName)){
            Scan scan=new Scan();
            ResultScanner results=table.getScanner(scan);
            for(Result result:results){
                printFormat(result);
            }
        }else{
            System.out.println("table is not exist!");
        }
    }
    //根据RowKey的起始值查找数据
    public static void scanData(String tableName,String startRow,String stopRow) throws IOException{
        Table table=connection.getTable(TableName.valueOf(tableName));
        if(tableIsExist(tableName)){
            Scan scan=new Scan();
            scan.setStartRow(Bytes.toBytes(startRow));
            scan.setStopRow(Bytes.toBytes(stopRow));
            ResultScanner results=table.getScanner(scan);
            for(Result result:results){
                printFormat(result);
            }
        }else{
            System.out.println("table is not exist!");
        }
    }
    //格式化输出
    public static void printFormat(Result result){
        Cell[] cells=result.rawCells();
        for(Cell cell:cells){
            System.out.print("RowKey:"+new String(CellUtil.cloneRow(cell))+"  ");
            System.out.print("ColumnFamily:"+new String(CellUtil.cloneFamily(cell))+"  ");
            System.out.print("ColumnName:"+new String(CellUtil.cloneQualifier(cell))+"  ");
            System.out.print("Value:"+new String(CellUtil.cloneValue(cell))+"  ");
            System.out.println("Timetamp:"+cell.getTimestamp());
        }
    }

    public static void main(String[] args) throws Exception{
        init();
        HbaseTest.clearTable("marineTest");
        close();
    }
}