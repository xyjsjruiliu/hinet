package com.xy.lr.spark.graphx

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{EdgeDirection, Graph}
import org.apache.spark.graphx.Graph._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

/**
 * Created by xylr on 15-5-27.
 */
object MyHits{

  def getLastIterationFile(sparkContext : SparkContext, lastIteration : String, graph : Graph[String, String]
                            ) : Graph[(Double, String), String] = {
    val rdd : RDD[(Long, Double)] = sparkContext.textFile(lastIteration)
      .map( x => x.substring(1, x.length-1)).map( x => (x.split(",")(0).toLong, x.split(",")(1).toDouble))

    val newGraph = graph.mapVertices( (id, attr) => (1.0, attr))
    val result : Graph[(Double, String), String] = newGraph.outerJoinVertices(rdd)( (vid, data, op) => {
      println("----------------------------\t" + op.get + "\t" + data._1 + "\t" + data._2)
      (op.get, data._2)
    })
    result
  }

  /*
  def run(graph : Graph[(Double, String), String], saveFilePath : String): Unit ={
    //initial the graph
    val initialGraph : Graph[(Double/*hits value*/ , String), String] = graph

    var hitsGraph = initialGraph.pregel(
      //initialMsg the message each vertex will receive at the on the first iteration
      initialMsg = (1.0, ""),
      //the maximum number of iterations
      1,
      //the direction of edges
      activeDirection = EdgeDirection.Out)(
        //the user-defined vertex program
        vprog = (vertexID, oldValue, newValue) => {
          //
          (newValue._1, oldValue._2)
        },
        /*sendMsg a user supplied function that is applied to out
        edges of vertices that received messages in the current iteration*/
        sendMsg = triplet => {
          @transient var defalutValue : Double = 1.0
          val defalutString : String = ""

          if(triplet.srcId < 20000000L && triplet.dstId< 20000000L){
            defalutValue = triplet.srcAttr._1 * triplet.attr.toDouble
          }else if (triplet.srcId < 20000000L && triplet.dstId < 30000000L ||
            triplet.srcId < 30000000L && triplet.dstId < 20000000L){
            defalutValue = triplet.srcAttr._1 / triplet.attr.toDouble
          }

          Iterator((triplet.dstId, (defalutValue, defalutString)))
        },
        /*mergeMsg a user supplied function that takes two incoming
        messages of type A and merges them into a single message of type A.*/
        mergeMsg = (a, b) => (a._1 + b._1, a._2)
      )

    val author = hitsGraph.vertices.filter{
      case(id, (key, value)) => if(id < 20000000L) true else false}.map( x => {
      ("1", x._2._1)
    }).reduceByKey(_+_).collect()(0)._2
    hitsGraph = hitsGraph.mapVertices( (id, attr) => {
      if(id < 20000000L) (attr._1 / author.toDouble, attr._2)
      else attr
    })

    val paper = hitsGraph.vertices.filter{
      case(id, (key, value)) => if(id > 20000000L && id < 30000000L) true else false}.map( x => {
      ("2", x._2._1)
    }).reduceByKey(_+_).collect()(0)._2
    hitsGraph = hitsGraph.mapVertices( (id, attr) => {
      if(id < 30000000L && id > 20000000L) (attr._1 / paper.toDouble, attr._2)
      else attr
    })

    val org = hitsGraph.vertices.filter{
      case(id, (key, value)) => if(id > 30000000L && id < 40000000L) true else false}.map( x => {
      ("3", x._2._1)
    }).reduceByKey(_+_).collect()(0)._2
    hitsGraph = hitsGraph.mapVertices( (id, attr) => {
      if(id < 40000000L && id > 30000000L) (attr._1 / org.toDouble, attr._2)
      else attr
    })

    val venue = hitsGraph.vertices.filter{
      case(id, (key, value)) => if(id > 40000000L) true else false}.map( x => {
      ("4", x._2._1)
    }).reduceByKey(_+_).collect()(0)._2
    hitsGraph = hitsGraph.mapVertices( (id, attr) => {
      if(id > 40000000L) (attr._1 / venue.toDouble, attr._2)
      else attr
    })
    hitsGraph.vertices.map( x => {
      (x._1, x._2._1)}).saveAsTextFile(saveFilePath)
  }*/

  def normalization(hitsGraph : Graph[(Double, String), String]): Graph[(Double, String), String] ={
    @transient var hits = hitsGraph

    val author = hits.vertices.filter{
      case(id, (key, value)) => if(id < 20000000L) true else false}.map( x => {
      ("1", x._2._1)
    }).reduceByKey(_+_).collect()(0)._2

    hits = hits.mapVertices( (id, attr) => {
      if(id < 20000000L) (attr._1 / author.toDouble, attr._2)
      else attr
    })

    val paper = hits.vertices.filter{
      case(id, (key, value)) => if(id > 20000000L && id < 30000000L) true else false}.map( x => {
      ("2", x._2._1)
    }).reduceByKey(_+_).collect()(0)._2
    hits = hits.mapVertices( (id, attr) => {
      if(id < 30000000L && id > 20000000L) (attr._1 / paper.toDouble, attr._2)
      else attr
    })

    val org = hits.vertices.filter{
      case(id, (key, value)) => if(id > 30000000L && id < 40000000L) true else false}.map( x => {
      ("3", x._2._1)
    }).reduceByKey(_+_).collect()(0)._2
    hits = hits.mapVertices( (id, attr) => {
      if(id < 40000000L && id > 30000000L) (attr._1 / org.toDouble, attr._2)
      else attr
    })

    val venue = hits.vertices.filter{
      case(id, (key, value)) => if(id > 40000000L) true else false}.map( x => {
      ("4", x._2._1)
    }).reduceByKey(_+_).collect()(0)._2
    hits = hits.mapVertices( (id, attr) => {
      if(id > 40000000L) (attr._1 / venue.toDouble, attr._2)
      else attr
    })

    hits
  }

  def run(sparkContext : SparkContext, last : String, graph : Graph[String, String], saveFilePath : String
           ,noThings : String): Graph[(Double, String), String] ={

    @transient var initialGraph : Graph[(Double/*hits value*/ , String), String] = null
    if(noThings.equals("1")){
      //initial the graph by set first 1.0 value
      initialGraph = graph.mapVertices(
        (id, attr) =>{
          (1.0, attr)
        }
      )
    }else{
      initialGraph = getLastIterationFile(sparkContext, last, graph)
    }


    @transient var hitsGraph = initialGraph.pregel(
      //initialMsg the message each vertex will receive at the on the first iteration
      initialMsg = (1.0, ""),
      //the maximum number of iterations
      1,
      //the direction of edges
      activeDirection = EdgeDirection.Out)(
        //the user-defined vertex program
        vprog = (vertexID, oldValue, newValue) => {
          if(newValue._2.equals("")){
            oldValue
          }else{
            (newValue._1, oldValue._2)
          }
        },
        /*sendMsg a user supplied function that is applied to out
        edges of vertices that received messages in the current iteration*/
        sendMsg = triplet => {
          @transient var defalutValue : Double = triplet.srcAttr._1
          val defalutString : String = "123"

          if(triplet.srcId < 20000000L && triplet.dstId< 20000000L){
            defalutValue = triplet.srcAttr._1 * triplet.attr.toDouble
          }else if (triplet.srcId < 20000000L && triplet.dstId < 30000000L ||
            triplet.srcId < 30000000L && triplet.dstId < 20000000L){
            defalutValue = triplet.srcAttr._1 / triplet.attr.toDouble
          }

          Iterator((triplet.dstId, (defalutValue, defalutString)))
        },
        /*mergeMsg a user supplied function that takes two incoming
        messages of type A and merges them into a single message of type A.*/
        mergeMsg = (a, b) => (a._1 + b._1, a._2)
      )
    //归一化
    hitsGraph = normalization(hitsGraph)

    hitsGraph.vertices.map( x => {
      (x._1, x._2._1)}).saveAsTextFile(saveFilePath)

    hitsGraph
  }
}
