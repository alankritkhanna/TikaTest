import java.io.*;
import java.util.Iterator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.input.PortableDataStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.BodyContentHandler;
import scala.Tuple2;

public class test {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);

        String filePath = "C:\\Users\\Alankrit\\Desktop\\docs\\*";
        extractContent(filePath);
    }

    public static void extractContent(String filePath){

        JavaSparkContext sc = new JavaSparkContext("local[*]", "Simple App");
        JavaPairRDD<String, PortableDataStream> data = sc.binaryFiles(filePath).repartition(8);
//        System.out.println(data.partitions().size());
//        JavaRDD<Tuple2<String, PortableDataStream>> javaRDD = sc.parallelize(data.collect());
//        System.out.println(javaRDD.partitions().size());

        data.foreachPartition((VoidFunction<Iterator<Tuple2<String, PortableDataStream>>>) tuple2Iterator -> {
            while(tuple2Iterator.hasNext()) {
                System.out.println("Partition ID: " + TaskContext.getPartitionId() + " ,Filename: " + tuple2Iterator.next()._1);
            }
        });

        long startTime = System.nanoTime();
        data.foreach(rdd -> {
//            System.out.println("Partition ID: " + TaskContext.getPartitionId());
            DataInputStream dis = new DataInputStream(rdd._2.open());
            BodyContentHandler handler = new BodyContentHandler();
            AutoDetectParser parser = new AutoDetectParser();
            Metadata metadata = new Metadata();
            ParseContext context = new ParseContext();

            try {
                parser.parse(dis, handler, metadata, context);
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println("Filename: " + rdd._1);
//            System.out.println("Content: " + handler.toString());
        });

        long elapsedTime = System.nanoTime() - startTime;
        System.out.println("Time: " + elapsedTime/1000000);
    }
}
