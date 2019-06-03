import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
//import org.joda.time.{DateTime, DateTimeZone}
import org.apache.log4j.Logger
import org.apache.log4j.Level

object goun {
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Join App").setMaster("local[4]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf) //it contains rdds (old way) START SPARK
    val spark = SparkSession.builder().config(conf).getOrCreate() //new way START SPARK
    val currentDir = System.getProperty("user.dir") // get the current directory
    val inputFile = "file://" + currentDir + "/input_file.txt"
    val outputDir = "file://" + currentDir + "/output/"

    /******* 1.RDD ******/
    //read file into an rdd
    val input_rdd = sc.textFile(inputFile, 2)

    //create rdd with R relations
    val r_relation = input_rdd.filter(line => line.charAt(0)=='R')
      .map(line => line.split(','))
      .map(line => (line(1), line(2)))

    //create rdd with S relations
    val s_relation = input_rdd.filter(line => line.charAt(0)=='S')
      .map(line => line.split(','))
      .map(line => (line(1), line(2)))

    //caclulate join's execution time
    val startTime = System.currentTimeMillis()
    //join the rdds (of R and S)
    val join_rdds = r_relation.join(s_relation)
    val endTime = System.currentTimeMillis()-startTime

    //write the results in a txt file
    join_rdds.saveAsTextFile(outputDir + "rdd/")

    //print the execution time and the number of results
    println("RDD \t Time: " + endTime + " milliseconds \t Number of results: " + join_rdds.count())

    /******* 2.Dataset ********/
    import spark.implicits._
    //read file into a dataset
    val input_dataset = spark.read.option("header", "false").option("delimiter", ',').textFile(inputFile)

    //create dataset with R relations
    val r2_relation = input_dataset.filter(line => line.charAt(0)=='R')
      .map(line => line.split(','))
      .map(line => (line(1), line(2)))

    //create dataset with S relations
    val s2_relation = input_dataset.filter(line => line.charAt(0)=='S')
      .map(line => line.split(','))
      .map(line => (line(1), line(2)))

    //calculate join's execution time
    val startTime2 = System.currentTimeMillis()
    //join the datasets (of R and S)
    val join_datasets = r2_relation.joinWith(s2_relation, r2_relation("_1") === s2_relation("_1"))
    val endTime2 = System.currentTimeMillis()-startTime2

    //write results in a json file (write.text throws exception)
    join_datasets.write.json(outputDir + "dataset/")

    //print execution time and number of results
    println("Dataset \t Time: " + endTime2 + " milliseconds \t Number of results: " + join_datasets.count())

    /****** 3.DATAFRAME *******/
    //read file into a dataframe
    val input_dataframe = spark.sqlContext.read.option("header", "false").option("delimiter", ",").csv(inputFile)

    //get second and third column
    val columns = Seq[String]("_c1", "_c2")

    //create two dataframes, one for each relation (R and S), and rename second column to key and third to value
    input_dataframe.filter("_c0 == 'R'").select(columns.head, columns.tail: _*)
      .toDF(Seq("key", "valueR"): _*)
      .createOrReplaceTempView("r_relation")
    input_dataframe.filter("_c0 == 'S'").select(columns.head, columns.tail: _*)
      .toDF(Seq("key", "valueS"): _*)
      .createOrReplaceTempView("s_relation")

    //calculate join's execution time
    val startTime3 = System.currentTimeMillis()
    //join the dataframes (of R and S) using an SQL query
    val join_dataframes = spark.sql("SELECT s_relation.key, s_relation.valueS, r_relation.valueR FROM s_relation INNER JOIN r_relation ON s_relation.key = r_relation.key")
    val endTime3 = System.currentTimeMillis()-startTime3

    //write results in a json file (write.text throws exception)
    join_dataframes.write.json(outputDir + "dataframes/")

    //print execution time and number of results
    println("DataFrame \t Time: " + endTime3 + " milliseconds \t Number of results: " + join_dataframes.count())

    /****** 4.RDD (manual join) ******/
    //read file into an rdd
    val input_rdd4 = sc.textFile(inputFile, 2)

    //calculate join's execution time
    val startTime4 = System.currentTimeMillis()

    //join operation
    val join_rdds4 = input_rdd4.map(line => line.split(','))
      //create combinations with the join key as key and the whole tuple as value
      .map(line => (line(1), (line(0), line(1), line(2))))
      //group these combinations by key
      .groupByKey()
      //keep only the value (whole tuple)
      .map(line => line._2)
      //filter two times and combine each record of R with all the records of S
      .flatMap(line => line.filter(r => r._1=="R").flatMap(tupleR => line.filter(s => s._1=="S")
      .map(tupleS => (tupleS, tupleR))))

    val endTime4 = System.currentTimeMillis()-startTime4

    //write the results in a txt file
    join_rdds4.saveAsTextFile(outputDir + "rdd_manual/")

    //print execution time and number of results
    println("Manual RDD  \t Time: " + endTime4 + " milliseconds \t Number of results: " + join_rdds4.count())

    /***** 5.RDD (manual join) vol.2 *****/
    //read file into an rdd
    val input_rdd5 = sc.textFile(inputFile, 2)

    //calculate join's execution time
    val startTime5 = System.currentTimeMillis()

    //join operation
    val join_rdds5 = input_rdd4.map(line => line.split(','))
      //create combinations with the join key as key and the whole tuple as value
      .map(line => (line(1), (line(0), line(1), line(2))))
      //group these combinations by key
      .groupByKey()
      //keep only the value (whole tuple)
      .map(line => line._2)
      //sort the relations by their distinctive
      .map(l => l.toList.sortWith(_._1 < _._1))
      //filter two times and combine each record of R with all the records of S
      .flatMap(line => line.filter(r => r._1=="R").flatMap(tupleR => line.filter(s => s._1=="S")
      .map(tupleS => (tupleS, tupleR))))

    val endTime5 = System.currentTimeMillis()-startTime5

    //write the results in a txt file
    join_rdds5.saveAsTextFile(outputDir + "rdd_manual_2/")

    //print execution time and number of results
    println("Manual RDD 2 \t Time: " + endTime5 + " milliseconds \t Number of results: " + join_rdds5.count())
  }

}

