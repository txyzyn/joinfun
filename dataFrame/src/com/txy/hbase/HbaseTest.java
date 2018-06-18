package com.txy.hbase;

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

/**
 * windows ��ִ��
 * @author Administrator
 *
 */
public class HbaseTest {
    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;

    public static void main(String[] args) throws IOException {
        createTable("hbase:kafka", new String[] { "info" });
        listTables();
        /*
         * insterRow("t2", "rw1", "cf1", "q1", "val1"); getData("t2", "rw1",
         * "cf1", "q1"); scanData("t2", "rw1", "rw2");
         * deleRow("t2","rw1","cf1","q1"); deleteTable("t2");
         */
    }

    // ��ʼ������
    public static void init() {
        configuration = HBaseConfiguration.create();
        /*
         * configuration.set("hbase.zookeeper.quorum",
         * "10.10.3.181,10.10.3.182,10.10.3.183");
         * configuration.set("hbase.zookeeper.property.clientPort","2181");
         * configuration.set("zookeeper.znode.parent","/hbase");
         */
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "master");
        configuration.set("hbase.master", "master:60000");
        File workaround = new File(".");
        System.getProperties().put("hadoop.home.dir",
                workaround.getAbsolutePath());
        new File("./bin").mkdirs();
        try {
            new File("./bin/winutils.exe").createNewFile();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // �ر�����
    public static void close() {
        try {
            if (null != admin)
                admin.close();
            if (null != connection)
                connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    // ����
    public static void createTable(String tableNmae, String[] cols) throws IOException {

        init();
        TableName tableName = TableName.valueOf(tableNmae);

        if (admin.tableExists(tableName)) {
            System.out.println("talbe is exists!");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            for (String col : cols) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
        }
        close();
    }

    // ɾ��
    public static void deleteTable(String tableName) throws IOException {
        init();
        TableName tn = TableName.valueOf(tableName);
        if (admin.tableExists(tn)) {
            admin.disableTable(tn);
            admin.deleteTable(tn);
        }
        close();
    }

    // �鿴���б�
    public static void listTables() throws IOException {
        init();
        HTableDescriptor hTableDescriptors[] = admin.listTables();
        for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
            System.out.println(hTableDescriptor.getNameAsString());
        }
        close();
    }

    // ��������
    public static void insterRow(String tableName, String rowkey, String colFamily, String col, String val)
            throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(val));
        table.put(put);

        // ��������
        /*
         * List<Put> putList = new ArrayList<Put>(); puts.add(put);
         * table.put(putList);
         */
        table.close();
        close();
    }

    // ɾ������
    public static void deleRow(String tableName, String rowkey, String colFamily, String col) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        // ɾ��ָ������
        // delete.addFamily(Bytes.toBytes(colFamily));
        // ɾ��ָ����
        // delete.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(col));
        table.delete(delete);
        // ����ɾ��
        /*
         * List<Delete> deleteList = new ArrayList<Delete>();
         * deleteList.add(delete); table.delete(deleteList);
         */
        table.close();
        close();
    }

    // ����rowkey��������
    public static void getData(String tableName, String rowkey, String colFamily, String col) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowkey));
        // ��ȡָ����������
        // get.addFamily(Bytes.toBytes(colFamily));
        // ��ȡָ��������
        // get.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(col));
        Result result = table.get(get);

        showCell(result);
        table.close();
        close();
    }

    // ��ʽ�����
    public static void showCell(Result result) {
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println("RowName:" + new String(CellUtil.cloneRow(cell)) + " ");
            System.out.println("Timetamp:" + cell.getTimestamp() + " ");
            System.out.println("column Family:" + new String(CellUtil.cloneFamily(cell)) + " ");
            System.out.println("row Name:" + new String(CellUtil.cloneQualifier(cell)) + " ");
            System.out.println("value:" + new String(CellUtil.cloneValue(cell)) + " ");
        }
    }

    // ������������
    public static void scanData(String tableName, String startRow, String stopRow) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        // scan.setStartRow(Bytes.toBytes(startRow));
        // scan.setStopRow(Bytes.toBytes(stopRow));
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            showCell(result);
        }
        table.close();
        close();
    }

}