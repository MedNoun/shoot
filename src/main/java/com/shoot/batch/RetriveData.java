package com.shoot.batch;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;

import com.google.gson.Gson;
import com.shoot.types.Card;
import com.shoot.types.Faul;
import com.shoot.types.Goal;
import com.shoot.types.Stat;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class RetriveData{

    public static void main(String[] args) throws IOException, Exception{
        // Instantiating Configuration class
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);

        String master = "local[*]";
        SparkConf config = new SparkConf()
                .setAppName(RetriveData.class.getName())
                .setMaster(master);

        JavaSparkContext sc = new JavaSparkContext(config);

        // Instantiating HTable class
        Table table = connection.getTable(TableName.valueOf("goal"));
        ArrayList<Card> cards = new ArrayList<Card>();
        ArrayList<Goal> goals = new ArrayList<Goal>();
        ArrayList<Faul> fauls = new ArrayList<Faul>();
        // Instantiating Get class
        Scan scan = new Scan(Bytes.toBytes("goal"));
        // Reading the data
        ResultScanner scanner = table.getScanner(scan);
        for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
            Stat s = new Stat();
            String str = "";
            String v = "";
            for (Cell cell : rr.rawCells()) {
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                if (column.equals("assist")) {
                    str = "goal";
                    v = value;
                } else if (column.equals("color")) {
                    str = "card";
                    v = value;
                } else if (column.equals("type")) {
                    str = "faul";
                    v= value;
                } else {
                    if(column.equals("player")){
                        s.player = value;
                    }
                    if(column.equals("timestamp")){
                        //s.timestamp = new Long(value);
                    }
                    if(column.equals("team")){
                        s.team = value;
                    }
                    if(column.equals("topic")){
                        //s.topic = value;
                    }

                }
            }
            if(str.equals("goal")){
                Goal g = new Goal();
                g.team = s.team;
                g.player = s.player;
                g.timestamp = s.timestamp;
                g.assist = v;
                //g.topic = str;
                goals.add(g);
            }
            if(str.equals("card")){
                Card g = new Card();
                g.team = s.team;
                g.player = s.player;
                g.timestamp = s.timestamp;
                g.color = v;
                //g.topic = str;
                cards.add(g);
            }
            if(str.equals("faul")){
                Faul g = new Faul();
                g.team = s.team;
                g.player = s.player;
                g.timestamp = s.timestamp;
                g.type = v;
                //g.topic = str;
                fauls.add(g);
            }
        }
        String team = "";
        String team1 = "";
        String team2 = "";
        int t1 = 0;
        int t2 = 0;
        for(Goal g : goals){
            if(!g.team.equals(team)){
                t1+=1;
                team = g.team;
                team1 = g.team;
            }else{
                t2+=1;
                team = g.team;
                team2= g.team;
            }
        }
        ArrayList<Tuple2<String,Integer>> info = new ArrayList<>();
        info.add(new Tuple2<>(team1,t1));
        info.add(new Tuple2<>(team1,t1));
        JavaRDD<Tuple2<String,Integer>> infoRDD = sc.parallelize(info);
        infoRDD.saveAsTextFile("output");


    }
}