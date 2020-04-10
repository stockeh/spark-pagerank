import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.HashPartitioner
import scala.collection.mutable.WrappedArray

object PageRank {

  def main(args: Array[String]){
    if (args.length < 1) {
      System.err.println(
        """Usage: <0 : idealized, 1 : taxation, 2 : wikibomb>
           |                         <links.txt>
           |                         <titles.txt>
        """.stripMargin)
      System.exit(1)
    }

    val option = args(0).toInt
    val output = "/tmp/out_" + args(0)
    val inputLinks = "/tmp/data/" + args(1)
    val inputNames = "/tmp/data/" + args(2)

    val spark = SparkSession
        .builder
        .appName("Page Rank v1.0")
        .getOrCreate()
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    // Map output link to each the given input, e.g., ((1, (2, 3, 4)), ...)
    var links = spark.read.textFile(inputLinks)
        .rdd.map{ s =>
          val parts = s.split(":")
          (parts(0), parts(1).trim().split("\\s+"))
        }.partitionBy(new HashPartitioner(100))
    // Assign indicies to names, e.g., ((1, A), (2, B), ...)
    val names = spark.read.textFile(inputNames)
        .rdd.zipWithIndex.map{ case (k,i) => ((i+1).toString, k)}

    if (option == 2) {
      val title_T = names.toDF("_1", "_2")
      title_T.createOrReplaceTempView("TITLE_TABLE")
      val link_T  = links.toDF("_1", "_2")
      link_T.createOrReplaceTempView("LINK_TABLE")
      // title_T.show()
      // link_T.show()
      // large-titles-sorted.txt:4290745:Rocky_Mountain_National_Park
      val fake = Array.fill(50)("4290745")
      val initialWebGraph = spark.sql("""
        SELECT _1 as id, _2 as link
        FROM LINK_TABLE WHERE _1
        IN (Select _1 FROM TITLE_TABLE WHERE _2 LIKE '%surfing%'
        OR _2 LIKE 'Rocky_Mountain_National_Park')
        """).rdd.map{ t =>
           ((t(0).toString, t(1).toString
                                .replaceAll("[^\\d+]", " ")
                                .trim().split("\\s+") ++ fake )) }
      links = initialWebGraph
      // links.collect().foreach(v => println(s"${v._1} - ${v._2.mkString(".")}"))
      // System.out.println("--HERE")
    }
    // Ranks of incoming pages, e.g., ((1, 1.0), ...)
    var ranks = links.mapValues(k => 1.0)
    // Taxation:  Beta -> (0.85), Alpha -> (1 - Beta)
    // Idealized: Beta -> (1.00), Alpha -> (0)
    val a = if (option == 0) 0.00 else 0.15
    val b = if (option == 0) 1.00 else 0.85
    val iters = 25
    // Compute Page Rank
    for (i <- 1 to iters) {
      val contribs = links.join(ranks).values.flatMap{
        case (urls, rank) =>
          urls.map(url => (url, rank / urls.size))
      }
      ranks = contribs.reduceByKey(_ + _)
          .mapValues(a + b * _)
    }
    val top = spark.sparkContext
        .parallelize(ranks.takeOrdered(10)(Ordering[Double].on(- _._2)))
    val out = top.join(names).sortBy(- _._2._1)
        .map{ case (k, v) => (v._2, (k, v._1)) }

    out.coalesce(1).saveAsTextFile(output)
    // out.collect().foreach(println)
  }
}
