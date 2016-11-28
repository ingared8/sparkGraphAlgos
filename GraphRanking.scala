package com.datascience.waze.spark.streetGraph.lineGraph

import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Graph, VertexRDD, Edge => GXEdge}
import org.apache.spark.sql.types.{IntegerType, LongType}
import org.graphframes.GraphFrame

/**
  * Created by greddy on 09/21/16.
  */
object GraphRanking {

  case class VertexAttr(srcId: Long, authScore: Double, hubScore:Double)

  case class EdgeAttr(srcId: Long, dstId: Long)

  case class HitsMsg(authScore:Double, hubScore:Double)

  case class PageMsg(pageScore:Double)

  def reducer(a:HitsMsg,b:HitsMsg):HitsMsg = HitsMsg(a.authScore + b.authScore, a.hubScore + b.hubScore)

  def runHits(g: GraphFrame, maxIter:Int = 10): GraphFrame = {

    val gx0 = g.toGraphX

    val vColsMap = g.vertexColumnMap
    val eColsMap = g.edgeColumnMap

    // Convert vertex attributes to nice case classes.
    // Initialize each node with hubScore = 1 and authScore = 1
    val gx1: Graph[VertexAttr, Row] = gx0.mapVertices { case (_, attr) =>
      VertexAttr(attr.getLong(vColsMap("id")), authScore = 1.0, hubScore = 1.0)
    }

    // Convert edge attributes to nice case classes.
    val extractEdgeAttr: (GXEdge[Row] => EdgeAttr) = { e =>
      val src = e.attr.getLong(eColsMap("src"))
      val dst = e.attr.getLong(eColsMap("dst"))
      EdgeAttr(src, dst)
    }

    var gx: Graph[VertexAttr, EdgeAttr] = gx1.mapEdges(extractEdgeAttr)
    for (iter <- Range(1,maxIter)) {
      val totalHubScores = gx.vertices
      val msgs: VertexRDD[HitsMsg] = gx.aggregateMessages(
        ctx =>
          // Can send to source or destination since edges are treated as undirected.
          {
            ctx.sendToDst(HitsMsg(0.0,ctx.srcAttr.hubScore));
            ctx.sendToSrc(HitsMsg(ctx.dstAttr.authScore,0.0))
          }, reducer)

      // Update authority and hub scores of each node
      gx = gx.outerJoinVertices(msgs) {
        case (vID, vAttr, optMsg) => {
          val msg = optMsg.getOrElse(HitsMsg(1.0, 1.0))
          println("Msg : " + msg.authScore + " , " + msg.hubScore)
          VertexAttr(vAttr.srcId, if (msg.authScore == 0.0) 1.0 else msg.authScore , if (msg.hubScore == 0.0) 1.0 else msg.hubScore)
          }
        }
      println("Iter ", iter)
    }

    // Convert back to GraphFrame with a new column "belief" for vertices DataFrame.
    // Inorder to deal with disconnected components
    val gxFinal: Graph[(Double,Double), Unit] = gx.mapVertices((_, attr) => (attr.authScore, attr.hubScore) )
      .mapEdges( _ => ())

    //gxFinal.edges.foreach(println)
    gxFinal.vertices.foreach(println)
    GraphFrame.fromGraphX(g, gxFinal, vertexNames = Seq("authScores", "hubScores"))
  }


  def runPageRank(g: GraphFrame, resetProb:Double = 0.2, maxIter:Int = 10): GraphFrame = {

    case class VertexAttr(srcId: Long, outDegree: Int, pageScore:Double)

    case class PageMsg(pageScore:Double)

    def reducer(a:PageMsg,b:PageMsg):PageMsg= PageMsg(a.pageScore + b.pageScore)

      val gx0 = g.toGraphX

      val vColsMap = g.vertexColumnMap
      val eColsMap = g.edgeColumnMap


      // Convert vertex attributes to nice case classes.
      // Initialize each node with hubScore = 1 and authScore = 1
      val gx1: Graph[VertexAttr, Row] = gx0.mapVertices { case (_, attr) =>
        VertexAttr(attr.getLong(vColsMap("id")), attr.getInt(vColsMap("outDegree")), resetProb)
      }

      val extractEdgeAttr: (GXEdge[Row] => EdgeAttr) = { e =>
        val src = e.attr.getLong(eColsMap("src"))
        val dst = e.attr.getLong(eColsMap("dst"))
        EdgeAttr(src, dst)
      }

      var gx: Graph[VertexAttr, EdgeAttr] = gx1.mapEdges(extractEdgeAttr)

      for (iter <- Range(1,maxIter)) {

        val msgs: VertexRDD[PageMsg] = gx.aggregateMessages (
          ctx =>
          ctx.sendToDst(PageMsg(ctx.srcAttr.pageScore / ( math.max(ctx.srcAttr.outDegree, 1)))),
          reducer )

        // Update page rank scores of each node
        gx = gx.outerJoinVertices(msgs) {
          case (vID, vAttr, optMsg) => {
            val msg = optMsg.getOrElse(PageMsg(0.0))
            VertexAttr(vAttr.srcId, vAttr.outDegree , resetProb + (1.0 - resetProb)*msg.pageScore)
          }
        }
        println("Iter ", iter)
      }

      // Convert back to GraphFrame with a new column "belief" for vertices DataFrame.
      // Inorder to deal with disconnected components
      val gxFinal: Graph[Double, Unit] = gx.mapVertices((_, attr) => attr.pageScore )
        .mapEdges( _ => ())

      //gxFinal.edges.foreach(println)
      gxFinal.vertices.foreach(println)
      GraphFrame.fromGraphX(g, gxFinal, vertexNames = Seq("pageRank"))
  }


//  def runInfluencePassivity(g: GraphFrame, resetProb:Double = 0.2, maxIter:Int = 10): GraphFrame = {
//
//
//  }

}

object HitsExample {

  import GraphRanking._

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("example").setMaster("local[2]")
    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = SQLContext.getOrCreate(sc)

    // Create graphical model g of size 3 x 3.
    val nodes = sqlContext
      .createDataFrame(List(
        (0, "A", 0.0, 0.0, 3, 0,0),
        (1, "a", 1.0, 1.0, 2, 1,1),
        (2, "b", 1.1, 1.0, 1, 1,2),
        (3, "c", 1.2, 1.0, 1, 1,3),
        (4, "d", 1.3, 1.0, 1, 1,4),
        (5, "e", 1.4, 1.0, 1, 1,5),
        (7, "B", 1.5, 1.1, 1, 1,6),
        (8, "C", 1.5, 1.2, 1, 1,7),
        (9, "D", 1.5, 1.3, 1, 1,8),
        (10, "E", 1.5, 1.4, 1, 1,9),
        (11, "F", 1.5, 1.5, 1, 1,10),
        (12, "Bb", 1.5, 2.1, 1, 1,11),
        (13, "Cc", 1.5, 2.2, 1, 1,12),
        (14, "Dd", 1.5, 2.3, 1, 1,13),
        (15, "Ee", 1.5, 2.4, 1, 1,14),
        (16, "Ff", 1.5, 2.5, 1, 1,15),
        (17, "Gg", 1.5, 2.6, 1, 1,16)))
      .toDF("index", "mark", "longitude", "latitude", "outDegree", "inDegree","colorInt")

    val toLongUdf = udf((i: Int) => i.toLong)
    val vertices = nodes.withColumn("id", toLongUdf(col("index"))).drop(col("index"))

    val connections =
      sqlContext.createDataFrame(List(
        (0, 1, 0.7),
        (0, 7, 0.4),
        (1, 2, 0.7),
        (2, 3, 0.2),
        (3, 4, 1.2),
        (4, 5, 1.3),
        (1, 7, 0.7),
        (7, 8, 0.4),
        (8, 9, 0.4),
        (9, 10, 0.4),
        (10, 11, 0.8),
        (0, 12, 0.5),
        (12, 13, 0.6),
        (13, 14 , 0.8),
        (14, 15, 0.7),
        (15, 16, 0.8),
        (16, 17, 0.9)))
        .toDF("srcIndex", "dstIndex", "dist")

    val edges = connections.withColumn("src",toLongUdf(col("srcIndex"))).withColumn("dst", toLongUdf(col("dstIndex")))
      .select(col("src"),col("dst"),col("dist"))
    val g = GraphFrame(vertices, edges)

    // Run BP for 5 iterations.
    println("Original Graph Model :")
    vertices.printSchema()

    println("Edges: ")
    edges.printSchema()

    val g2 = runHits(g,maxIter = 20)

    g2.vertices.show()

    // Done with Coloring
    sc.stop()
  }

}

object pageRankExample {

  import GraphRanking._

  def main(args: Array[String]): Unit = {
    //val conf = new SparkConf().setAppName("Graph Frame Example Loading").setMaster("local[1]")
    val conf = new SparkConf().setAppName("example").setMaster("local[2]")
    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = SQLContext.getOrCreate(sc)
    sc.setLogLevel("WARN")


    val toLongUdf = udf((i: Int) => i.toLong)

    val vertices = sqlContext
      .createDataFrame(List(
        (1, "Extended Stay America", "6531 S Sepulveda Blvd", 10.0, -180.0, -1.832, 1, 2),
        (2, "DataScience, Inc.", "200 Corporate Point", 20.0, -30.0, 1.233, 0, 1),
        (3, "7157 Alvern Street", "Culver City", 20.0, 30.0, 1.00, 1, 1),
        (4, "USC", "South Vermont", -32.0, 60.0, -2.765, 1, 1),
        (5, "UCLA", "Los Angeles", -64.0, 122.89, 1.246, 0, 0))
      ).toDF("id1", "name", "address", "x", "y", "a", "i", "outDegree")
      .withColumn("id", toLongUdf(col("id1"))).drop(col("id1"))

    val edges =
      sqlContext.createDataFrame(List(
        (1, 2, 0.7332),
        (2, 1, 0.26737821),
        (3, 4, 1.2324),
        (4, 3, 1.32131),
        (1, 3, 3.2323))).toDF("src1", "dst1", "b")
        .withColumn("src", toLongUdf(col("src1")))
        .withColumn("dst", toLongUdf(col("dst1")))

    //val edgesWithWeights = edges.withColumn("weight", col("src") + col("dst"))
    val g = GraphFrame(vertices, edges)
    //testAllFunctionalities(g)
    //runBP(g)(iter)(writeEnabled)
    val pageRankResults = g.pageRank.resetProbability(0.15).tol(0.01).run()
    pageRankResults.vertices.select("id", "pagerank").show()
    pageRankResults.edges.select("src", "dst", "weight").show()


    val g2 = runPageRank(g)
    g2.vertices.select("id", "pageRank").show()
    g2.edges.show()

    sc.stop()
  }
}
