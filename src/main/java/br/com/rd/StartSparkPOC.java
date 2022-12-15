package br.com.rd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Objects;

public class StartSparkPOC {

    public static void main(String[] args) {
        System.out.println("Iniciando a aplicação para a POC Apache Spark");
        if (args.length<3){
            System.out.println("Não foram passados todos os parametros");
            System.out.println("executar: javaSparkContext -jar SparkPOC.jar 'aplicationName' 'address-spark-cluster' 'address-dataset-csv' ");
            System.out.println("aplicationName = nome da aplicação");
            System.out.println("address-spark-cluster = endereço do cluster spark passando o protocolo ex.: spark:\\address:7077");
            System.out.println("address-dataset-csv =  Endereço do csv para processamento");
            System.out.println("access-key =  Chave de acesso ao serviço S3");
            System.out.println("secret-key =  secret de acesso ao serviço S3");
            return;
        }
        String appName = args[0];
        String addressCluster = args[1];
        String dataSetPath = args[2];
        String accessKeyS3 = null;
        String secretKeyS3 = null;
        String urlLocation = null;
        if (args.length>3){
            accessKeyS3 = args[3];
        }
        if (args.length>4){
            secretKeyS3 = args[4];
        }
        if (args.length>5){
            urlLocation = args[5];
        }




        System.out.println("Processo configurado com:");
        System.out.println("Application name:"+appName);
        System.out.println("Cluster address:"+addressCluster);
        System.out.println("Dataset address:"+dataSetPath);

        var sparkConf = new SparkConf().setAppName("beneficiary-load-process")
                .setMaster("emr".equals(addressCluster.toLowerCase())? null: addressCluster)
                .setAppName(appName)
//                .set("spark.driver.allowMultipleContexts","true")
//                .set("spark.shuffle.service.enabled", "false")
//                .set("spark.dynamicAllocation.enabled", "false")
                ;

        var javaSparkContext = new JavaSparkContext(sparkConf);
        javaSparkContext.hadoopConfiguration()
                .set("fs.s3a.connection.maximum", "10");

        if (Objects.nonNull(accessKeyS3)){
            javaSparkContext.hadoopConfiguration()
                    .set("fs.s3a.access.key", accessKeyS3);
        }

        if (Objects.nonNull(secretKeyS3)){
            javaSparkContext.hadoopConfiguration()
                    .set("fs.s3a.secret.key", secretKeyS3);
        }

        if (Objects.nonNull(urlLocation)){
            javaSparkContext.hadoopConfiguration()
                    .set("hive.metastore.warehouse.dir", urlLocation);
            javaSparkContext.hadoopConfiguration()
                    .set("spark.sql.warehouse.dir", urlLocation);
        }


        SparkSession spark = SparkSession.builder()
                .sparkContext(javaSparkContext.sc())
                .getOrCreate();

        Dataset<Row> dataset = spark.read()
                .option("header", true)
                .option("delimiter",";")
                .csv(dataSetPath);

        dataset.createOrReplaceTempView("dados");

        System.out.println("====================Inicio=====================");

        spark.sql("select count(*) " +
                " from dados " ).toJSON().collectAsList().forEach(System.out::println);
        System.out.println("====================Processamento finalizado=====================");
    }


}

