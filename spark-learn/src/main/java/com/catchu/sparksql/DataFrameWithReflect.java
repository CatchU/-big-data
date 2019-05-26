package com.catchu.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * 通过反射的方式创建dataframe
 */
public class DataFrameWithReflect {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("DataFrameWithReflect");
        conf.setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        JavaRDD<String> lineRdd = sc.textFile("./sparksql/person.txt");
        JavaRDD<Person> personRdd = lineRdd.map(new Function<String, Person>() {
            @Override
            public Person call(String line) throws Exception {
                Person p = new Person()
                        .setId(Long.valueOf(line.split(",")[0]))
                        .setName(line.split(",")[1])
                        .setAge(Integer.valueOf(line.split(",")[2]));
                return p;
            }
        });

        //通过反射的方式将rdd转化成dataframe
        Dataset<Row> personDf = sqlContext.createDataFrame(personRdd, Person.class);
        personDf.show();
        personDf.printSchema();

        //注册临时表，实际并不雾化到磁盘
        personDf.registerTempTable("tb");
        sqlContext.sql("select * from tb where tb.age>18").show();

        //将dataframe转化成rdd
        JavaRDD<Row> rowJavaRDD = personDf.javaRDD();
        JavaRDD<Person> personRdd2 = rowJavaRDD.map(new Function<Row, Person>() {
            @Override
            public Person call(Row row) throws Exception {
                return new Person()
                        .setId(row.getAs("id"))
                        .setName(row.getAs("name"))
                        .setAge(row.getAs("age"));
            }
        });
        personRdd2.foreach(new VoidFunction<Person>() {
            @Override
            public void call(Person person) throws Exception {
                System.out.println(person);
            }
        });

        sc.close();
    }
}
