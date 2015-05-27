package com.xy.lr.spark

import com.xy.lr.spark.graphx.MyHits
import org.apache.spark.graphx.Graph
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
 * Created by xylr on 15-5-27.
 */
object yidaMain {

  //make eachVertexRDD
  def getEachVertexRDDFromFile(sparkContext : SparkContext,
                               filePath : String, vertexName : String, addend : Long
                                ) : RDD[(VertexId, String)] = {
    val vertexRDD : RDD[(VertexId, String)] = sparkContext.textFile(filePath + vertexName).map(
      x => {
        //vertexId
        val index = x.split("\t")(0).toLong + addend
        (index, x)
      })
    vertexRDD
  }

  def getAuthorVertexRDD(sparkContext : SparkContext,
                         filePath : String, vertexName : String, addend : Long) : RDD[(VertexId, String)] = {
    val vertexRDD : RDD[(VertexId, String)] = sparkContext.textFile(filePath + vertexName).map(
      x => {
        //vertexId
        val index = x.split("\t")(0).toLong + addend
        (index, x)
      })
    vertexRDD
  }

  //make VertexRDD
  def getVertexRDD(sparkContext : SparkContext, filePath : String
                    ) : RDD[(VertexId, String)] = {
    //authorVertexRDD	1000W
    val authorVertexRDD : RDD[(VertexId, String)] =
      getEachVertexRDDFromFile(sparkContext, filePath, "author", 10000000L)
    //paperVertexRDD	2000W
    val paperVertexRDD : RDD[(VertexId, String)] =
      getEachVertexRDDFromFile(sparkContext, filePath, "paper", 20000000L)
    //orgVertexRDD	3000W
    val orgVertexRDD : RDD[(VertexId, String)] =
      getEachVertexRDDFromFile(sparkContext, filePath, "org", 30000000L)
    //venueVertexRDD	4000W
    val venueVertexRDD : RDD[(VertexId, String)] =
      getEachVertexRDDFromFile(sparkContext, filePath, "venue", 40000000L)

    //union each vertex
    val unionVertexRDD = authorVertexRDD.union(paperVertexRDD)
      .union(orgVertexRDD).union(venueVertexRDD)
    unionVertexRDD
  }

  //make eachEdgeRDD
  def getEachEdgeRDDFromFile(sparkContext : SparkContext, filePath : String,
                             edgeName : String, sourceAddend : Long, destAddend : Long, property : String,
                             fanxiangFlag : Boolean) : RDD[Edge[String]] = {
    val edgeRDD : RDD[Edge[String]] = sparkContext.textFile(filePath + edgeName).map(
      x => {
        @transient var sourceAuthorId : Long = 1L
        @transient var destAuthorId : Long = 1L
        if(!fanxiangFlag){
          //source vertexId
          sourceAuthorId = x.split("\t")(1).toLong + sourceAddend
          //dest vertexId
          destAuthorId = x.split("\t")(2).toLong + destAddend
        }
        else{
          //source vertexId
          sourceAuthorId = x.split("\t")(2).toLong + destAddend
          //dest vertexId
          destAuthorId = x.split("\t")(1).toLong + sourceAddend
        }

        //property
        @transient var fea = ""
        if(property.equals("NULL")){
          fea = "null"
        }
        else{
          fea = x.split("\t")(3)
        }
        new Edge(sourceAuthorId, destAuthorId, fea)
      })
    //return
    edgeRDD
  }

  //make EdgeRDD
  def getEdgeRDD(sparkContext : SparkContext, filePath : String
                  ) : RDD[Edge[String]] = {
    //coauthor
    val coauthorRDD : RDD[Edge[String]] =
      getEachEdgeRDDFromFile(sparkContext, filePath, "coauth_vertex", 10000000L, 10000000L, "not NULL", false)
    val coauthorReverseRDD : RDD[Edge[String]] =
      getEachEdgeRDDFromFile(sparkContext, filePath, "coauth_vertex", 10000000L, 10000000L, "not NULL", true)

    //citate bu fanxiang
    val citateEdgeRDD : RDD[Edge[String]] =
      getEachEdgeRDDFromFile(sparkContext, filePath, "citate_vertex", 20000000L, 20000000L, "NULL", false)

    //publish
    val publishEdgeRDD : RDD[Edge[String]] =
      getEachEdgeRDDFromFile(sparkContext, filePath, "publish_vertex", 20000000L, 40000000L, "NULL", false)
    val publishReverseEdgeRDD : RDD[Edge[String]] =
      getEachEdgeRDDFromFile(sparkContext, filePath, "publish_vertex", 20000000L, 40000000L, "NULL", true)

    //work
    val workEdgeRDD : RDD[Edge[String]] =
      getEachEdgeRDDFromFile(sparkContext, filePath, "work_vertex", 10000000L, 30000000L, "NULL", false)
    val workReverseEdgeRDD : RDD[Edge[String]] =
      getEachEdgeRDDFromFile(sparkContext, filePath, "work_vertex", 10000000L, 30000000L, "NULL", true)

    //wrote
    val wroteEdgeRDD : RDD[Edge[String]] =
      getEachEdgeRDDFromFile(sparkContext, filePath, "wrote_vertex", 10000000L, 20000000L, "not NULL", false)
    val wroteReverseEdgeRDD : RDD[Edge[String]] =
      getEachEdgeRDDFromFile(sparkContext, filePath, "wrote_vertex", 10000000L, 20000000L, "not NULL", true)

    //union each edge
    val unionEdgeRDD = coauthorRDD.union(citateEdgeRDD).union(publishEdgeRDD)
      .union(workEdgeRDD).union(wroteEdgeRDD).union(coauthorReverseRDD).union(publishReverseEdgeRDD)
      .union(workReverseEdgeRDD).union(wroteReverseEdgeRDD)
    //return
    unionEdgeRDD
  }

  //main function
  def main(args : Array[String]) : Unit = {
    if(args.length != 3){
      println("Usage : spark-submit " +
        "--master spark://localhost:7077 " +
        "--class com.xy.lr.scala.ldy.spark.yidaMain " +
        "hdfs://hadoop-server:9000/user/hadoop/ " +
        "hdfs://hadoop-server:9000/hi_index 1")
      System.exit(0)
    }
    //conf
    val sparkConf = new SparkConf()
      .setMaster("spark://hadoop-server:7077")
      .setAppName("lindayi")
    //context
    val sparkContext = new SparkContext(sparkConf)

    //hdfs file path of vertex
    val filePath : String = args(0)

    val VertexRDD : RDD[(VertexId, String)] = getVertexRDD(sparkContext, filePath)
    val EdgeRDD : RDD[Edge[String]] = getEdgeRDD(sparkContext, filePath)

    //the graph
    val graph : Graph[String, String] = Graph(VertexRDD, EdgeRDD)
    //    graph.cache()

    MyHits.run(graph, args(2).toInt, args(1))
    //    graph.vertices.map( x => {
    //      println(x._2)
    //    })
  }
}