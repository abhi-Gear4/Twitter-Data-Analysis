import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Twitter {
    def main(args: Array[ String ]) {
    val conf = new SparkConf().setAppName("twitter")
    val sc = new SparkContext(conf)
    val e = sc.textFile(args(0)).map( line => { val a = line.split(",")
                                                (a(1).toInt,1) } )
    val res1 = e.reduceByKey(_+_)
    val res2 = res1.map(l=> (l._2.toInt,1))
    val res3 = res2.reduceByKey(_+_).sortByKey()
    res3.collect().foreach(println(_))
    sc.stop()
  }
}
