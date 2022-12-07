package br.com.rd;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class StartSparkPOC {

    public static void main(String[] args) {
        System.out.println("Iniciando a aplicação para a POC Apache Spark");
        if (args.length<3){
            System.out.println("Não foram passados todos os parametros");
            System.out.println("executar: java -jar SparkPOC.jar 'aplicationName' 'address-spark-cluster' 'address-dataset-csv' ");
            System.out.println("aplicationName = nome da aplicação");
            System.out.println("address-spark-cluster = endereço do cluster spark passando o protocolo ex.: spark:\\address:7077");
            System.out.println("address-dataset-csv =  Endereço do csv para processamento");
            return;
        }
        String appName = args[0];
        String addressCluster = args[1];
        String dataSetPath = args[2];

        System.out.println("Processo configurado com:");
        System.out.println("Application name:"+appName);
        System.out.println("Cluster address:"+addressCluster);
        System.out.println("Dataset address:"+dataSetPath);

        SparkSession spark = SparkSession.builder()
                .appName(appName)
                .master("emr".equals(addressCluster.toLowerCase())? null: addressCluster)
                //.config("spark.sql.warehouse.dir",warehouseLocation)
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
