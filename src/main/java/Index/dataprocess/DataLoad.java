package Index.dataprocess;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

/*
 * Function：把一个File中的数据全部加载到HBASE中
 */
public class DataLoad {

    public static final String tableName="marine";
    public static final String columnFamily="property";

    /*
     * 数据表头：
     * Identification,Latitude,Longitude,Time of Observation,Ice Accretion On Ship,Thickness of Ice Accretion On Ship,
     * Rate of Ice Accretion on Ship,Sea Level Pressure,Characteristics of Pressure Tendency,Pressure Tendency,
     * Air Temperature,Wet Bulb Temperature,Dew Point Temperature,Sea Surface Temperature,
     * Wave Direction,Wave Period,Wave Height,Swell Direction,Swell Period,Swell Height,
     * Total Cloud Amount,Low Cloud Amount,Low Cloud Type,Cloud Height Indicator,Cloud Height,
     * Middle Cloud Type,High Cloud Type,Visibility,Visibility Indicator,Present Weather,Past Weather,Wind Direction,Wind Speed
     */
    //数据逐条插入
    public static void dataInsert(File file) throws IOException{
        BufferedReader br=new BufferedReader(new InputStreamReader(new FileInputStream(file)));
        ArrayList<String> dataOfLine=new ArrayList<>();//保存一行数据
        ArrayList<String> tableHead=new ArrayList<>();//保存每个字段的名字
        tableHead.add("RowKey");
        String line=br.readLine();
        if(line!=null){
            String[] fieldName=line.trim().replace(" ", "").split(",");//读取头文件
            for(String field:fieldName){
                tableHead.add(field);
            }
        }

        while((line=br.readLine())!=null){
            dataOfLine.add("0");//初始化Rowkey
            String[] fields=line.trim().replace(" ", "").split(",");
            for(String field:fields){
                dataOfLine.add(field);
            }
            double latitude=Double.valueOf(dataOfLine.get(2));
            double longitude=Double.valueOf(dataOfLine.get(3));
            String hilbertCode=HilbertCode.toBinaryCode(longitude, latitude, 10);
            String date=dataOfLine.get(4);
            //rowkey=希尔伯特编码+日期
            String rowkey=hilbertCode+date;
            dataOfLine.set(0, rowkey);
            if(!HbaseTest.tableIsExist(tableName)){
                System.out.println("Table is not exist!");
                return;
            }
            for(int i=1;i<dataOfLine.size();i++){
                String field=dataOfLine.get(i);
                String columnName=tableHead.get(i);
                if(!field.equals("")){
                    HbaseTest.insterRow(tableName, rowkey, columnFamily, columnName, field);
                }
            }
            dataOfLine.clear();
        }
        tableHead.clear();

        br.close();
    }
    //批量插入数据 file为根目录
    public static void bulkInsert(File file) throws IOException{
        File[] flist=file.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                if(pathname.getName().equals("Standard")){
                    return false;
                }
                return true;
            }
        });
        if(flist.length==0){
            System.out.println("空文件夹");
            return;
        }
        for(File f:flist){
            if(f.isDirectory()){
                bulkInsert(f);
            }else{
                System.out.println("正在加载"+f.getName()+"中的数据");
                dataInsert(f);
                System.out.println("已加载");
            }
        }
    }
    public static void main(String[] args) throws IOException{

		/*File file=new File("F:\\软件\\海洋数据\\selenium\\数据集\\DATA\\20100501_20100530_492URL\\120100501_20100530_492URL.csv");
		dataInsert(file);*/
        long startTime = System.currentTimeMillis();
        HbaseTest.init();
        File file=new File("C:\\Users\\Administrator\\Desktop\\selenium\\数据集\\DATA\\20100101_20100301_841URL");
        try {
            bulkInsert(file);
        } catch (IOException e) {
            e.printStackTrace();
        }
        HbaseTest.close();
        long endTime = System.currentTimeMillis();
        System.out.println("程序运行时间：" + (endTime - startTime) + "ms"+"  "+(endTime - startTime)*1.0/1000 + "s");
    }
}
