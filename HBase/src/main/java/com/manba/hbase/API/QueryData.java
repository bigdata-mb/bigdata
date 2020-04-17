package com.manba.hbase.API;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

public class QueryData {
    private static Connection connection ;
    private final static String TABLE_NAME = "myuser";


    public static void main(String[] args) throws IOException {

        //操作数据库  第一步：获取连接  第二步：获取客户端对象   第三步：操作数据库  第四步：关闭
        Configuration configuration = HBaseConfiguration.create();
        //连接HBase集群不需要指定HBase主节点的ip地址和端口号
        //node01 node02 node03 为主机名
        configuration.set("hbase.zookeeper.quorum","node01:2181,node02:2181,node03:2181");
        //创建连接对象
        connection = ConnectionFactory.createConnection(configuration);

        // 按照rowkey进行查询，获取所有列的所有值
        // 查询主键rowkey为0003的人
        QueryData.getData();
        // 不知道rowkey的具体值，我想查询rowkey范围值是0003  到0006
        // select * from myuser  where age > 30  and id < 8  and name like 'zhangsan'
        QueryData.scanData();
    }

    /**
     * 查询rowkey为0003的人
     */
    public static void getData() throws IOException {
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        //通过get对象，指定rowkey
        Get get = new Get(Bytes.toBytes("0003"));

        get.addFamily("f1".getBytes());//限制只查询f1列族下面所有列的值
        //查询f2  列族 phone  这个字段
        get.addColumn("f2".getBytes(),"phone".getBytes());
        //通过get查询，返回一个result对象，所有的字段的数据都是封装在result里面了
        Result result = table.get(get);
        List<Cell> cells = result.listCells();  //获取一条数据所有的cell，所有数据值都是在cell里面 的
        for (Cell cell : cells) {
            byte[] family_name = CellUtil.cloneFamily(cell);//获取列族名
            byte[] column_name = CellUtil.cloneQualifier(cell);//获取列名
            byte[] rowkey = CellUtil.cloneRow(cell);//获取rowkey
            byte[] cell_value = CellUtil.cloneValue(cell);//获取cell值
            //需要判断字段的数据类型，使用对应的转换的方法，才能够获取到值
            if("age".equals(Bytes.toString(column_name))  || "id".equals(Bytes.toString(column_name))){
                System.out.println(Bytes.toString(family_name));
                System.out.println(Bytes.toString(column_name));
                System.out.println(Bytes.toString(rowkey));
                System.out.println(Bytes.toInt(cell_value));
            }else{
                System.out.println(Bytes.toString(family_name));
                System.out.println(Bytes.toString(column_name));
                System.out.println(Bytes.toString(rowkey));
                System.out.println(Bytes.toString(cell_value));
            }
        }
        table.close();
    }

    /**
     * 不知道rowkey的具体值，我想查询rowkey范围值是0003  到0006
     * select * from myuser  where age > 30  and id < 8  and name like 'zhangsan'
     *
     */
    public static void scanData() throws IOException {
        //获取table
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        Scan scan = new Scan();//没有指定startRow以及stopRow  全表扫描
        //只扫描f1列族
        scan.addFamily("f1".getBytes());
        //扫描 f2列族 phone  这个字段
        scan.addColumn("f2".getBytes(),"phone".getBytes());
        scan.setStartRow("0003".getBytes());
        scan.setStopRow("0007".getBytes());
        //通过getScanner查询获取到了表里面所有的数据，是多条数据
        ResultScanner scanner = table.getScanner(scan);
        //遍历ResultScanner 得到每一条数据，每一条数据都是封装在result对象里面了
        for (Result result : scanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                byte[] family_name = CellUtil.cloneFamily(cell);
                byte[] qualifier_name = CellUtil.cloneQualifier(cell);
                byte[] rowkey = CellUtil.cloneRow(cell);
                byte[] value = CellUtil.cloneValue(cell);
                //判断id和age字段，这两个字段是整形值
                if("age".equals(Bytes.toString(qualifier_name))  || "id".equals(Bytes.toString(qualifier_name))){
                    System.out.println("数据的rowkey为" +  Bytes.toString(rowkey)   +"======数据的列族为" +  Bytes.toString(family_name)+"======数据的列名为" +  Bytes.toString(qualifier_name) + "==========数据的值为" +Bytes.toInt(value));
                }else{
                    System.out.println("数据的rowkey为" +  Bytes.toString(rowkey)   +"======数据的列族为" +  Bytes.toString(family_name)+"======数据的列名为" +  Bytes.toString(qualifier_name) + "==========数据的值为" +Bytes.toString(value));
                }
            }
        }
        table.close();
    }

}
