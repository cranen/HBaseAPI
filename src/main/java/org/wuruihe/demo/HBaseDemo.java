package org.wuruihe.demo;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.Op;

import javax.sound.midi.Soundbank;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

public class HBaseDemo  {
    private static Configuration conf;
    private static  Connection connection;
    private  static  Admin admin;
    static{
        //使用hbaseconfiguration 的单例方法
        conf= HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","192.168.10.201");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            connection= ConnectionFactory.createConnection(conf);
            admin= connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    //1判断表是否存在
    public static boolean isTableExist(String tablename) throws Exception {
        boolean b = admin.tableExists(TableName.valueOf(tablename));
        return b;
    }
    //2创建表
    public static void createTable(String tableName, String... columnFamily) throws Exception {
        HBaseAdmin admin=new HBaseAdmin(conf);
        //判断表是否存在
        if(isTableExist(tableName)){
            System.out.println("表"+tableName+"已存在");
           // return;
        }else{
            //创建表属性对象，表名需要转义字符
            HTableDescriptor descriptor= new HTableDescriptor(TableName.valueOf(tableName));
            //创建多个列族
            for(String cf:columnFamily){
                descriptor.addFamily(new HColumnDescriptor(cf));
            }

            //根据对表的配置，创建表
            admin.createTable(descriptor);
            System.out.println("表"+tableName+"创建成功！");
        }


    }

    //3删除表
    public static void dropTable(String tablename) throws Exception {
        if(isTableExist(tablename)){
            //System.out.println("不存在！");
            TableName tableName = TableName.valueOf(tablename);
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }else{
            System.out.println("表不存在");
        }

    }

    //4插入一条数据
    public static void createDate(String tableName,String rowkey,String cf,String cn,String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put=new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn),Bytes.toBytes(value));

        table.put(put);
    }

    //5插入多条数据
    public static void  createtables(String tablename,String rowkey, String cf,String []cn,String []vale) throws IOException {
        //获取表对象
        Table table = connection.getTable(TableName.valueOf(tablename));
        Put put =new Put(Bytes.toBytes(rowkey));
        //向put对象中封装数据
        for(int i=0; i<cn.length;i++){
            put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn[i]),Bytes.toBytes(vale[i]));
        }
        //执行插入工作
        table.put(put);
        table.close();

    }

    //6向表中插入一行数据
    public static void  getrow(String tablename,String rowkey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tablename));
        //获得get对象
        Get get=new Get(Bytes.toBytes(rowkey));
        //设置值的最大版本数
        get.setMaxVersions();
        //执行回去数据的操作
        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));//获取列族
            System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));//获取列明
            System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));//获取列值
            System.out.println(cell.getTimestamp());

        }

        table.close();

    }

    //7获取一条数据
    private  static void getRow(String tablename,String rowkey) throws IOException {
        //获取表对象
        Table table = connection.getTable(TableName.valueOf(tablename));
        //获取get对象
        Get get = new Get(Bytes.toBytes(rowkey));
        //执行获取数据操作
        get.setMaxVersions();
        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        for(Cell cell:cells){

            System.out.println( "rowkeys:"+ Bytes.toString(CellUtil.cloneRow(cell)) +",cf"+Bytes.toString(CellUtil.cloneFamily(cell)));

            System.out.print("，columnName"+Bytes.toString(CellUtil.cloneQualifier(cell)));
            //此为时间戳
            System.out.println(cell.getTimestamp());

        }
        table.close();

    }

    //8获取指定列族 的人

    public static void getRowByCf(String tablename,String rowkey,String cf,String  cn) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tablename));
        Get get =new Get(Bytes.toBytes(rowkey));
       // get.addFamily(Bytes.toBytes(cf));
        get.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));
        //设置最大的版本

        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println(rowkey);
            System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));

        }
        table.close();

    }
    //9扫描一张表
    private static void scanTable(String tablename) throws IOException {
        //获取表对象
        Table table = connection.getTable(TableName.valueOf(tablename));
        Scan scan=new Scan();
        ResultScanner scanner = table.getScanner(scan);
     //   Iterator<Result> iterator = scanner.iterator();

        for (Result result : scanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {

                System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));

            }
        }
        table.close();
    }

    //删除一条数据
    public static void deleteDate(String tableName,String rowKey,String cf,String cn) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete=new Delete(Bytes.toBytes(rowKey));
        delete.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));
        //执行删除操作
        table.delete(delete);
        table.close();

    }

    //删除多条数据（多个rowkey）
    private static void deleteDates(String tablename,String ...rowkey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tablename));
        ArrayList<Delete> deletes=new ArrayList<Delete>();
        for (String  row : rowkey) {
            Delete delete1=new Delete(Bytes.toBytes(row));
            deletes.add(delete1);
        }
        table.delete(deletes);
        table.close();
    }


    public static void main(String[] args) throws Exception {
          // isTableExist("wrh");
      //  createTable("stu_wrh","info");
      //  getRow("wrh","1009");
       // dropTable("stu_wrh");//stu_wrh
        createDate("stu_wrh","10010","info","name","wrh01");
    }

}
