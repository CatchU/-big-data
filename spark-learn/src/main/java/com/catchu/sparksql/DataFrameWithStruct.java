package com.catchu.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

/**
 * 通过字段类型创建dataframe
 */
public class DataFrameWithStruct {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("DataFrameWithStruct");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        JavaRDD<String> linesRdd = sc.textFile("./sparksql/person.txt");
        JavaRDD<Row> personRdd = linesRdd.map(new Function<String, Row>() {
            @Override
            public Row call(String line) throws Exception {
                return RowFactory.create(Long.valueOf(line.split(",")[0]),
                        line.split(",")[1],
                        Integer.parseInt(line.split(",")[2]));
            }
        });
        List<StructField> fields = Arrays.asList(
                DataTypes.createStructField("id", DataTypes.LongType, true),
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.IntegerType, true)
        );
        StructType schema = DataTypes.createStructType(fields);
        Dataset<Row> df = sqlContext.createDataFrame(personRdd, schema);
        df.show();

        JavaRDD<Row> rows = df.javaRDD();
        rows.foreach(new VoidFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
                System.out.println(row);
            }
        });
        sc.close();
    }
}
