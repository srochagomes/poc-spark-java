package br.com.rd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

public class StartSparkPOC {

    public static void main(String[] args) {

        String appName = "Sparc POC";
        String addressCluster = "emr";

        String dataSetPath = "s3a://s3-emr-univers-spark-dev/dados.csv";
        String urlLocation = "s3a://s3-emr-univers-spark-dev";


        System.out.println("Processo configurado com:");
        System.out.println("Application name:"+appName);
        System.out.println("Cluster address:"+addressCluster);
        System.out.println("Dataset address:"+dataSetPath);

        SparkConf sparkConf = new SparkConf().setAppName("beneficiary-load-process")

                .setAppName(appName);

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        if (Objects.nonNull(urlLocation)){
            javaSparkContext.hadoopConfiguration()
                    .set("hive.metastore.warehouse.dir", urlLocation);
            javaSparkContext.hadoopConfiguration()
                    .set("spark.sql.warehouse.dir", urlLocation);
        }



        SparkSession spark = SparkSession.builder()
                .sparkContext(javaSparkContext.sc())
                .enableHiveSupport()
                .getOrCreate();

        Dataset<Row> dataset = spark.read()
                .option("header", true)
                .option("delimiter",";")
                .csv(dataSetPath);

        dataset.createOrReplaceTempView("dados");

        System.out.println("====================Inicio=====================");

        String data = LocalDateTime.now().format(DateTimeFormatter.ISO_DATE);
        spark.sql("select * " +
                " from dados " )
                .toJSON();
//                .write().mode(SaveMode.Overwrite)
//                .json(urlLocation+"/resultado_lista_".concat(data));

        spark.sql("select count(*) " +
                " from dados " )
                .toJSON().collectAsList().forEach(System.out::println);
//                .write().mode(SaveMode.Overwrite)
//                .json(urlLocation+"/resultado_tamanho_".concat(data));

        System.out.println("====================Processamento finalizado=====================");

        javaSparkContext.close();
    }



}

