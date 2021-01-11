
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession, functions}
import org.apache.spark.sql.functions.{array_contains, col, desc, explode, size, when}

import scala.math.Ordering.Implicits.infixOrderingOps

object Main {


  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().master("local[*]").appName("hi").getOrCreate();
    session.sparkContext.setLogLevel("ERROR")
    //print All
    val df = session.read.json("src/main/songs/names.json")
    df.show()
    //print schema
    df.printSchema()

    //print types all columns
    df.dtypes.foreach(println)

    //Add column salary the value is (age)*(quantity of technologies)*10
    val salary = df.withColumn("salary", col("age") * (size(df.col("keywords"))) * 10)
    salary.persist();
    salary.show()

    //Find Salary less then 1200 and when developer knows the popular technology
    val lowSalary = salary.filter(col("salary") <= 1200)
    lowSalary.show()
    val technology =  salary.withColumn("technology", explode(col("keywords")))
    technology.show()
    val popular = df
      .withColumn("technology", explode(col("keywords")))
      .groupBy("technology")
      .count()
      .sort(desc("count"))
      .first()





  val res = technology
    .filter(col("salary") <= 1200)
    .filter(functions.array_contains(col("keywords"), popular.get(0)))

      res.show()



  }
}
