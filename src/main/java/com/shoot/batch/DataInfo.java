package com.shoot.batch;

import javafx.util.Pair;
import org.apache.hadoop.fs.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import static jersey.repackaged.com.google.common.base.Preconditions.checkArgument;
public class DataInfo {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataInfo.class);


    public static void main(String[] args) {
        checkArgument(args.length > 1, "Please provide the path of input file and output dir as parameters.");
        new DataInfo().run(args[0], args[1]);
    }
    public ArrayList<String> listFiles(String inputPath, JavaSparkContext sc){
        try{
            Path path = new Path(inputPath);
            RemoteIterator<LocatedFileStatus> i = path.getFileSystem(sc.hadoopConfiguration()).listFiles(path, true);
            ArrayList<String> files = new ArrayList();
            while (i.hasNext()){
                LocatedFileStatus f = i.next();
                files.add(f.getPath().toString());
            }
            return files;
        }catch(IOException e){
            throw new RuntimeException(e);
        }
    }
    public ArrayList<String> listDirectories(String inputPath, JavaSparkContext sc) {
        try{
            ArrayList<String> directories = new ArrayList();
            Path path = new Path(inputPath);
            FileStatus[] status = path.getFileSystem(sc.hadoopConfiguration()).listStatus(path);
            Iterator<FileStatus> i = Arrays.stream(status).iterator();
            while(i.hasNext()){
                FileStatus f = i.next();
                if(f.isDirectory()){
                    directories.add(f.getPath().toString());
                }
            }
            return directories;
        }catch(IOException e){
            throw new RuntimeException(e);
        }

    }
    public void run(String inputDirectoryPath, String outputDir) {
        String master = "local[*]";
        SparkConf conf = new SparkConf()
                .setAppName(DataInfo.class.getName())
                .setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);
        ArrayList<Tuple2<String,Integer>> info = new ArrayList<>();
        for(String element : this.listDirectories(inputDirectoryPath,sc)){
            ArrayList<String> files = this.listFiles(element,sc);
            info.add(new Tuple2(element, files.size()));
        };
        JavaRDD<Tuple2<String,Integer>> infoRDD = sc.parallelize(info);
        infoRDD.saveAsTextFile(outputDir);
    }
}