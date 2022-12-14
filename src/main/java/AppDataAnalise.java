import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class AppDataAnalise {

    public static void main(String[] args) {
        System.out.println("Init");
        System.out.println("Creation of Spark Session");
        SparkSession spark = SparkSession
                .builder()
                .appName("Test")
                .config("spark.master", "local")
                .getOrCreate();
        System.out.println("Spark Session Created");
        String url = Constants.HOST + ":"+Constants.PORT+"/"+Constants.DATABASE_NAME;
        System.out.println("Reading from source: "+url);

        Dataset<Row> df = spark.read()
                .format("jdbc")
                .option("driver", "org.postgresql.Driver")
                .option("url", "jdbc:postgresql://"+url)
                .option("dbtable", Constants.DB_TABLE)
                .option("user", Constants.DB_USER)
                .option("password", Constants.DB_PASS)
                .load();

        System.out.println("Dataset read: ");
        df.show();
        //We are making a temp view from the df we read
        System.out.println("Creation of temp view");
        df.createOrReplaceTempView(Constants.DB_TABLE);
        System.out.println("First query --> max_loan_num_age_range");
        Dataset<Row> df_metric_1 = spark.sql("select t.range as age_ranges, count(*) as counter\n" +
                "from (\n" +
                "  select case  \n" +
                "    when CAST (b.age as INTEGER) between 0 and 30 then '0-30'\n" +
                "    when CAST (b.age as INTEGER) between 30 and 40 then '30-40'\n" +
                "    when CAST (b.age as INTEGER) between 40 and 50 then '40-50'\n" +
                "    when CAST (b.age as INTEGER) between 50 and 60 then '50-60'\n" +
                "    else '60-99' end as range\n" +
                "  from "+Constants.DB_TABLE+" b\n" +
                "  where loan='yes') t\n" +
                "group by t.range").orderBy(col("counter").desc()).limit(1);

        //Now we are going to make a common format to store the data in a table
        Dataset<Row> df_metric_1_formatted=df_metric_1.withColumn("metric_name",lit("max_loan_num_age_range"))
                .withColumnRenamed("counter","value")
                .withColumnRenamed("age_ranges","key")
                .select("metric_name","key","value");
        df_metric_1_formatted.show();
        //For next metrics we will implement everything in sql
        //We are using the avg to give the true value of the balance here, because the group numbers are heterogeneous
        System.out.println("Second query --> max_balance_age_range_marital_status");
        Dataset<Row> df_metric_2 = spark.sql("select 'max_balance_age_range_marital_status' as name,concat(t.range,'|',t.marital) as key, avg(CAST (balance as INTEGER)) as value\n" +
                "from (\n" +
                "  select case  \n" +
                "    when CAST (b.age as INTEGER) between 0 and 30 then '0-30'\n" +
                "    when CAST (b.age as INTEGER) between 30 and 40 then '30-40'\n" +
                "    when CAST (b.age as INTEGER) between 40 and 50 then '40-50'\n" +
                "    when CAST (b.age as INTEGER) between 50 and 60 then '50-60'\n" +
                "    else '60-99' end as range,\n" +
                "    b.marital,b.balance\n" +
                "  from "+Constants.DB_TABLE+" b) t\n" +
                "group by t.range,t.marital"
        ).orderBy(col("value").desc()).limit(1);
        df_metric_2.show();
        System.out.println("Third query --> most_common_contact_between_ages_25_35");
        Dataset<Row> df_metric_3 = spark.sql(
                "select 'most_common_contact_between_ages_25_35' as name,contact as key, count(1) as value\n" +
                "        from (\n" +
                "                select case\n" +
                "                when CAST (b.age as INTEGER) between 25 and 35 then '25-35'\n" +
                "    else 'else' end as range,\n" +
                "                contact\n" +
                "        from "+Constants.DB_TABLE+" b) t\n" +
                "        where t.range = '25-35'\n" +
                "        group by t.range,contact"
        ).orderBy(col("value").desc()).limit(1);
        df_metric_3.show();
        System.out.println("Fourth query --> max_balance_for_each_campaign_with_marrital_and_job, min_balance_for_each_campaign_with_marrital_and_job and avg_balance_for_each_campaign_with_marrital_and_job");
        Dataset<Row> df_metric_4 = spark.sql("with t as (select CONCAT(campaign,'|',marital,'|',job) as key,balance\n" +
                "    from bank_1 b\n" +
                "    )\n" +
                "select 'max_balance_for_each_campaign_with_marrital_and_job' as name,T.key, MAX(cast(BALANCE as INTEGER)) as value from t group by T.KEY\n" +
                "union all\n" +
                "select 'min_balance_for_each_campaign_with_marrital_and_job' as name,T.key, min(cast(BALANCE as INTEGER)) as value from t group by T.KEY\n" +
                "union all\n" +
                "select 'avg_balance_for_each_campaign_with_marrital_and_job' as name,T.key, avg(cast(BALANCE as INTEGER)) as value from t group by T.KEY"
        );
        df_metric_4.show();

        System.out.println("Fifth query --> most_common_job_for_married_housing_campaign_3_with_more_than_1200_balance, min_balance_for_each_campaign_with_marrital_and_job and avg_balance_for_each_campaign_with_marrital_and_job");
        Dataset<Row> df_metric_5 = spark.sql("select 'most_common_job_for_married_housing_campaign_3_with_more_than_1200_balance' as name,job as key, count(1) as value\n" +
                "from (\n" +
                "  select *\n" +
                "    from bank_1 b\n" +
                "    where \n" +
                "    marital='married'\n" +
                "    and housing='yes'\n" +
                "    and cast(balance as INTEGER) > 1200\n" +
                "    and cast(campaign as INTEGER)=3\n" +
                "    ) t\n" +
                "group by t.job"
        ).orderBy(col("value").desc()).limit(1);
        df_metric_5.show();

        System.out.println("Now we merge all data into one dataframe and insert it in "+Constants.DB_TABLE_METRICS);
        Dataset<Row> df_metrics = df_metric_1_formatted
               .union(df_metric_2)
               .union(df_metric_3)
               .union(df_metric_4)
               .union(df_metric_5);
        //Now we process all the metrics
        df_metrics.write()
                .format("jdbc")
                .option("driver", "org.postgresql.Driver")
                .option("url", "jdbc:postgresql://"+url)
                .option("dbtable", Constants.DB_TABLE_METRICS)
                .option("user", Constants.DB_USER)
                .option("password", Constants.DB_PASS)
                .mode("overwrite")
                .option("truncate","true")
                .save();

    }
}
