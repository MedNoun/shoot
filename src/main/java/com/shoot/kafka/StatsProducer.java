package com.shoot.kafka;


import java.util.Date;
import java.util.Properties;
import java.util.Random;

import com.google.gson.Gson;
import com.shoot.types.Card;
import com.shoot.types.Faul;
import com.shoot.types.Goal;
import com.shoot.types.Stat;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;



public class StatsProducer {
    public static void main(String[] args) throws Exception{
        // Verifier que le topic est donne en argument
        if(args.length < 4){
            System.out.println("Entrer le nom du topic");
            return;
        }
        String topicName = args[0].toString();

        String key = RandomStringUtils.randomAlphanumeric(10);
        // Assigner topicName a une variable

        // Creer une instance de proprietes pour acceder aux configurations du producteur
        Properties props = new Properties();

        // Assigner l'identifiant du serveur kafka
        props.put("bootstrap.servers", "localhost:9092");

        // Definir un acquittement pour les requetes du producteur
        props.put("acks", "all");

        // Si la requete echoue, le producteur peut reessayer automatiquemt
        props.put("retries", 0);

        // Specifier la taille du buffer size dans la config
        props.put("batch.size", 16384);

        // buffer.memory controle le montant total de memoire disponible au producteur pour le buffering
        props.put("buffer.memory", 33554432);

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        if(topicName.equals("card")){
            Card c = new Card();
            c.topic = "card";
            c.player = args[1].toString();
            c.team = args[2].toString();
            c.color = args[3].toString();
            c.timestamp = new Date().getTime();
            producer.send(new ProducerRecord<String, String>(topicName, key, new Gson().toJson(c)));
        }
        if(topicName.equals("faul")){
            Faul c = new Faul();
            c.topic = "faul";
            c.player = args[1].toString();
            c.team = args[2].toString();
            c.type = args[3].toString();
            c.timestamp = new Date().getTime();
            producer.send(new ProducerRecord<String, String>(topicName, key, new Gson().toJson(c)));
        }
        if(topicName.equals("goal")){
            Goal c = new Goal();
            c.topic = "goal";
            c.player = args[1].toString();
            c.team = args[2].toString();
            c.assist = args[3].toString();
            c.timestamp = new Date().getTime();
            producer.send(new ProducerRecord<String, String>(topicName, key, new Gson().toJson(c)));
        }



        producer.close();
    }
}
