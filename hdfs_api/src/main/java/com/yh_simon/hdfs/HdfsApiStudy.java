package com.yh_simon.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsApiStudy {


    //小文件的合并
    @Test
    public void mergeFileTest() throws URISyntaxException, IOException, InterruptedException {
        //获取FileSystem对象
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration(), "root");
        //在hdfs创建一个大文件
        FSDataOutputStream outputStream = fileSystem.create(new Path("/bigFile.txt"));
        //获取本地的文件系统
        LocalFileSystem localFileSystem = FileSystem.getLocal(new Configuration());
        //获取本地文件下所有文件的信息
        FileStatus[] fileStatuses = localFileSystem.listStatus(new Path("D://01input"));
        //遍历数据，获取每个文件的信息
        for(FileStatus fileStatus:fileStatuses){
            FSDataInputStream inputStream = localFileSystem.open(fileStatus.getPath());
            IOUtils.copy(inputStream, outputStream);
            IOUtils.closeQuietly(inputStream);
        }
        IOUtils.closeQuietly(outputStream);
        localFileSystem.close();
        fileSystem.close();
    }


    //实现文件的上传
    @Test
    public void uploadFileTest() throws  URISyntaxException,IOException{
        //1.获取FileSystem对选哪个
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration());
        //2.调用方法实现文件的上传
        fileSystem.copyFromLocalFile(new Path("C://27.jpg"), new Path("/"));
        fileSystem.close();
    }

    //实现文件的下载  方式二
    @Test
    public void downloadFileTest2() throws  URISyntaxException,IOException{
        //1.获取FileSystem对象
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration());
        //2.调用方法直接实现文件的下载
        fileSystem.copyToLocalFile(new Path("/b.txt"), new Path("D://b1.txt"));
        fileSystem.close();
    }

    //实现文件的下载  方式一
    @Test
    public void downloadFileTest1() throws URISyntaxException, IOException {
        //1.获取FileSystem对象
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration());
        //2.获取HDFS文件的输入流
        FSDataInputStream inputStream = fileSystem.open(new Path("/b.txt"));
        //3.获取本地文件的输出流
        FileOutputStream fileOutputStream = new FileOutputStream(new File(("D://b.txt")));
        //4.实现文件的复制
        IOUtils.copy(inputStream, fileOutputStream);
        //5.关闭流
        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(fileOutputStream);
        fileSystem.close();
    }

    //在hdfs上创建文件夹(文件)
    @Test
    public void mkdirsTest() throws URISyntaxException, IOException {
        //1.获取FileSystem对象
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration());
        //2.创建文件夹
       // fileSystem.mkdirs(new Path("/app/test/hello"));

        //3.创建文件
        fileSystem.create(new Path("/a.txt"));

    }

    //获取目录下所有的文件信息
    @Test
    public void listFiles() throws URISyntaxException, IOException, InterruptedException {
        //获取FileSystem对象
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration(),"root");
        //获取指定目录下的所有文件信息，第二个参数表示是否要递归遍历
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path("/"), true);
        while(iterator.hasNext()){
            //获取每一个文件详细信息
            LocatedFileStatus fileStatus=iterator.next();
            //获取每一个文件的存储路径
            System.out.println(fileStatus.getPath()+"----"+fileStatus.getPath().getName());//hdfs://
            //获取文件的Block存储信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            //打印每个文件的block数目
            System.out.println(blockLocations.length);
            //打印每一个block副本的存储位置
            for (BlockLocation blockLocation:blockLocations){
                String[] hosts=blockLocation.getHosts();
                for (String host:hosts){
                    System.out.println(host+"-");
                }
            }
        }


    }

    @Test
    public void getFileSystem1() throws IOException {
        Configuration configuration = new Configuration();
        //指定需要操作的文件系统
        configuration.set("fs.defaultFS", "hdfs://node01:8020");
        //获取指定的文件系统，获取FileSystem相当于获取了主节点中的所有元数据信息
        FileSystem fileSystem = FileSystem.get(configuration);
        System.out.println("fileSystem:"+fileSystem.toString());
    }

    @Test
    public void getFileSystem2() throws IOException, URISyntaxException {
        FileSystem fileSystem = FileSystem.get(new URI("/"), new Configuration());
        System.out.println(fileSystem.toString());
    }

    @Test
    public void getFileSystem3() throws IOException {
        Configuration configuration=new Configuration();
        configuration.set("fs.defaultFS", "hdfs://node01:8020");
        FileSystem fileSystem = FileSystem.newInstance(configuration);
        System.out.println(fileSystem);
    }

    @Test
    public void getFileSystem4() throws URISyntaxException, IOException {
        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://node01:8020"), new Configuration());
        System.out.println(fileSystem);
    }
}
