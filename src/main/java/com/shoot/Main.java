package com.shoot;

import com.shoot.kafka.StatsProducer;

public class Main {
    public static void main(String[] args) {
        try{
            System.out.println(args[0]);
            if(args[0].equals("consumer")){
                System.out.println("consuming");
            }else{
                System.out.println("producing");
            }
        }catch(Exception e){
            e.printStackTrace();
        }

    }
}