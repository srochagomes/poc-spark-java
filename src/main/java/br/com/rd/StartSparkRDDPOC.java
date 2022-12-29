package br.com.rd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class StartSparkRDDPOC {


    public static void main(String[] args) {
        System.out.println("Iniciando a aplicação para a POC Apache Spark");
        if (args.length<3){
            System.out.println("Não foram passados todos os parametros");
            System.out.println("executar: javaSparkContext -jar SparkPOC.jar 'aplicationName' 'address-spark-cluster' 'address-dataset-csv' ");
            System.out.println("aplicationName = nome da aplicação");
            System.out.println("address-spark-cluster = endereço do cluster spark passando o protocolo ex.: spark:\\address:7077");
            System.out.println("address-dataset-csv =  Endereço do csv para processamento");
            System.out.println("url-location =  Localização base S3");
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
            urlLocation = args[3];
        }
        if (args.length>4){
            accessKeyS3 = args[4];
        }
        if (args.length>5){
            secretKeyS3 = args[5];
        }




        System.out.println("Processo configurado com:");
        System.out.println("Application name:"+appName);
        System.out.println("Cluster address:"+addressCluster);
        System.out.println("Dataset address:"+dataSetPath);

        SparkConf sparkConf = new SparkConf().setAppName("beneficiary-load-process")
                .setMaster("emr".equals(addressCluster.toLowerCase())? null: addressCluster)
                .setAppName(appName)
//                .set("spark.driver.allowMultipleContexts","true")
//                .set("spark.shuffle.service.enabled", "false")
//                .set("spark.dynamicAllocation.enabled", "false")
                ;

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
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




        System.out.println("====================Inicio=====================");

        int slices = (args.length == 1) ? Integer.parseInt(args[0]) : 2;
        int n = 100000 * slices;
        List<Integer> l = new ArrayList<Integer>(n);
        for (int i = 0; i < n; i++) {
            l.add(i);
        }

        JavaRDD<Integer> dataSet = javaSparkContext.parallelize(l, slices);

        int count = dataSet.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) {
                double x = Math.random() * 2 - 1;
                double y = Math.random() * 2 - 1;
                return (x * x + y * y < 1) ? 1 : 0;
            }
        }).reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) {
                return integer + integer2;
            }
        });

        System.out.println("Pi is roughly " + 4.0 * count / n);

        javaSparkContext.stop();

        System.out.println("====================Processamento finalizado=====================");
    }


}

