package sql_practice

import org.apache.spark.sql.functions._
import spark_helpers.SessionBuilder

object examples {
  def exec1(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val toursDF = spark.read
      .option("multiline", true)
      .option("mode", "PERMISSIVE")
      .json("data/input/tours.json")
    toursDF.show

    println(toursDF
      .select(explode($"tourTags"))
      .groupBy("col")
      .count()
      .count()
    )

    toursDF
      .select(explode($"tourTags"), $"tourDifficulty")
      .groupBy($"col", $"tourDifficulty")
      .count()
      .orderBy($"count".desc)
      .show(10)

    toursDF.select($"tourPrice")
      .filter($"tourPrice" > 500)
      .orderBy($"tourPrice".desc)
      .show(20)


  }

  def exec3(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val s07DF = spark.read
      .option("mode", "PERMISSIVE")
      .option("delimiter","\t")
      .csv("data/input/sample_07")
      .withColumnRenamed("_c0","code_07")
      .withColumnRenamed("_c1","description_07")
      .withColumnRenamed("_c2","total_emp_07")
      .withColumnRenamed("_c3","salary_07")

    val s08DF = spark.read
      .option("mode", "PERMISSIVE")
      .option("delimiter", "\t")
      .csv("data/input/sample_08")
      .withColumnRenamed("_c0", "code_08")
      .withColumnRenamed("_c1", "description_08")
      .withColumnRenamed("_c2", "total_emp_08")
      .withColumnRenamed("_c3", "salary_08")

    //s07DF.show
    //s08DF.show

    //1. Find top salaries in 2007 which are above $100k
      s07DF.select($"description_07", $"salary_07")
      .filter($"salary_07" > 100000)
      .orderBy($"salary_07".desc)
      .show(20)


    //2. Find salary growth (sorted) from 2007-08
    val join = s07DF.join(s08DF, s07DF("code_07") === s08DF("code_08"))

    join.withColumn("salaryGrowth", join("salary_08") - join("salary_07"))
      .select($"code_07", $"description_07",$"salary_07",$"salary_08", $"salaryGrowth")
      .sort($"salaryGrowth".desc)
      .show(20)

    //3. Find jobs loss among the top earnings from 2007-08
    //val join2 = s07DF.join(s08DF, s07DF("code_07") === s08DF("code_08"))

    val top2007_8 = join.withColumn("salaryGrowth", join("salary_08") - join("salary_07"))
      .sort($"salaryGrowth".desc)

      top2007_8.withColumn("diffEmp", join("total_emp_07") - join("total_emp_08"))
      .select($"code_07", $"description_07", $"total_emp_07", $"total_emp_08", $"diffEmp")
      .sort($"diffEmp".desc)
      .show(20)
  }
}
