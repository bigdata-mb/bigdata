package com.manba.hdfs.API;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSOperate {

    @Test
    public void mkdir() throws IOException {
        //配置项
        Configuration configuration = new Configuration();

        //设置要连接的hdfs集群
        configuration.set("fs.defaultFS", "hdfs://node01:8020");

        //获得文件系统
        FileSystem fileSystem = FileSystem.get(configuration);

        //调用方法创建目录
        boolean mkdirs = fileSystem.mkdirs(new Path("/mkdir0106"));

        //释放资源
        fileSystem.close();
    }

    @Test
    public void mkdir2() throws IOException, URISyntaxException, InterruptedException {
        //配置项
        Configuration configuration = new Configuration();

        //设置要连接的hdfs集群
        //configuration.set("fs.defaultFS", "hdfs://node01:8020");

        //获得文件系统
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), configuration, "test");

        //调用方法创建目录
        boolean mkdirs = fileSystem.mkdirs(new Path("/mkdir010601"));

        //释放资源
        fileSystem.close();
    }

    @Test
    public void uploadFile() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://node01:8020");

        FileSystem fileSystem = FileSystem.get(configuration);

        fileSystem.copyFromLocalFile(new Path("file:///C:\\Users\\admin\\Desktop\\高级05\\HDFS第二次\\core3.xml"),
                new Path("hdfs://node01:8020/kaikeba/dir1"));
        fileSystem.close();
    }

    @Test
    public void downloadFile() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://node01:8020");
        FileSystem fileSystem = FileSystem.get(configuration);
//        fileSystem.delete()
//        fileSystem.rename()
        fileSystem.copyToLocalFile(new Path("hdfs://node01:8020/kaikeba/dir1/core3.xml"),new Path("file:///C:\\Users\\admin\\Desktop\\高级05\\HDFS第二次\\core3.xml"));

        fileSystem.close();
    }

    @Test
    public void testListFiles() throws IOException, InterruptedException, URISyntaxException{
        // 1获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://node01:8020"), configuration);

        // 2 获取文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/dir1/core3.xml"), true);
        while(listFiles.hasNext()){
            LocatedFileStatus status = listFiles.next();
            // 输出详情
            // 文件名称
            System.out.println(status.getPath().getName());
            // 长度
            System.out.println(status.getLen());
            // 权限
            System.out.println(status.getPermission());
            // 分组
            System.out.println(status.getGroup());
            // 获取存储的块信息
            BlockLocation[] blockLocations = status.getBlockLocations();

            for (BlockLocation blockLocation : blockLocations) {
                // 获取块存储的主机节点
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
        }
        // 3 关闭资源
        fs.close();
    }

    @Test
    public void putFileToHDFS() throws IOException, InterruptedException, URISyntaxException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://node01:8020"), configuration);
        // 2 创建输入流 不需要加file:///
        FileInputStream fis = new FileInputStream(new File("C:\\Users\\admin\\Desktop\\高级05\\HDFS第二次\\core.xml"));
        // 3 获取输出流
        FSDataOutputStream fos = fs.create(new Path("hdfs://node01:8020/core4.xml"));
        // 4 流对拷
        IOUtils.copy(fis, fos);
        // 5 关闭资源
        IOUtils.closeQuietly(fos);
        IOUtils.closeQuietly(fis);
        fs.close();
    }

    /**
     * 小文件合并
     */
    @Test
    public   void  mergeFile() throws URISyntaxException, IOException, InterruptedException {
        //获取分布式文件系统hdfs
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration(), "hadoop");

        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("hdfs://node01:8020/bigfile.xml"));

        //获取本地文件系统 localFileSystem
        LocalFileSystem localFileSystem = FileSystem.getLocal(new Configuration());
        //读取本地的文件
        FileStatus[] fileStatuses = localFileSystem.listStatus(new Path("file:///C:\\Users\\admin\\Desktop\\高级05\\HDFS第二次\\小文件合并"));
        for (FileStatus fileStatus : fileStatuses) {
            //获取每一个本地的文件路径
            Path path = fileStatus.getPath();
            //读取本地小文件
            FSDataInputStream fsDataInputStream = localFileSystem.open(path);

            IOUtils.copy(fsDataInputStream,fsDataOutputStream);
            IOUtils.closeQuietly(fsDataInputStream);
        }
        IOUtils.closeQuietly(fsDataOutputStream);
        localFileSystem.close();
        fileSystem.close();
        //读取所有本地小文件，写入到hdfs的大文件里面去
    }



}
