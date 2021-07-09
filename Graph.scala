import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object Graph {
  val start_id = 14701391
  val max_int = Int.MaxValue
  val iterations = 5
 
  def main ( args: Array[ String ] ) {
    val conf = new SparkConf().setAppName("Graph")
    val sc = new SparkContext(conf)

    val graph: RDD[(Int,Int)]
         = sc.textFile(args(0))
             .map(l => {val a = l.split(","); (a(1).toInt,a(0).toInt)})                // create a graph edge (i,j), where i follows j

    var R: RDD[(Int,Int)]             // initial shortest distances
         = graph.groupByKey()
                .map(zoro => { if(zoro._1 == 1 || zoro._1 == start_id ) (zoro._1,0); else(zoro._1,max_int)})          // starting point has distance 0, while the others max_int

    for (i <- 0 until iterations) {
       R = R.join(graph)
            .flatMap(tup => {if(tup._2._1 < max_int)List((tup._2._2,tup._2._1+1),(tup._1,tup._2._1)) else List((tup._1,tup._2._1))})         // calculate distance alternatives
            .reduceByKey((a, b) => if (a < b) a; else b)     // for each node, find the shortest distance
    }

    R.filter(a => a._2<max_int )                 // keep only the vertices that can be reached
     .sortByKey()
     .collect()
     .foreach(println)
  }
}


