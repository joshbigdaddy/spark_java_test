import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class AppIngestionPostgres {


    public static void main(String[] args) {
        System.out.println("Init");
        System.out.println("Creation of Spark Session");
        SparkSession spark = SparkSession
                .builder()
                .appName("Test")
                .config("spark.master", "local")
                .getOrCreate();
        System.out.println("Spark Session Created");

        System.out.println("Reading from source: "+Constants.PATH);

        Dataset<Row> df = spark.read().option("delimiter", Constants.DELIMITER).option("header", "true").csv(Constants.PATH);
        System.out.println("Dataset read: ");
        df.show();
        String url = Constants.HOST + ":"+Constants.PORT+"/"+Constants.DATABASE_NAME;
        System.out.println("Writing to host DB URL: "+ url);
        //We are overwriting the information so we do not have to mind if we are truncating or not the information
        df.write()
                .format("jdbc")
                .option("driver", "org.postgresql.Driver")
                .option("url", "jdbc:postgresql://"+url)
                .option("dbtable", Constants.DB_TABLE)
                .option("user", Constants.DB_USER)
                .option("password", Constants.DB_PASS)
                .mode("overwrite")
                .option("truncate","true")
                .save();

    }
}
