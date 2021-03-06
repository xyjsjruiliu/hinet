package com.xy.lr.scala.ldy.spark

import org.apache.spark.graphx._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

/**
 * Created by xylr on 15-5-19.
 */
object MyHits{


  def filterAuthor(tuple2: Tuple2[VertexId, (Double, String)]) : Boolean = {
    true
  }

  def run(graph : Graph[String, String], maxIterations: Int = Int.MaxValue): Graph[(Double, String), String] ={

    //initial the graph by set first 1.0 value
    val initialGraph : Graph[(Double/*hits value*/ , String), String] = graph.mapVertices(
      (id, attr) =>{
        (1.0, attr)
      }
    )
    @transient var hitsGraph : Graph[(Double, String), String] = null

    @transient var t : Int = 0
    while(t < maxIterations){
      hitsGraph = initialGraph.pregel(
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
              defalutValue = triplet.srcAttr._1 * triplet.toString()(2)
            }else if (triplet.srcAttr._1 < 20000000L.toDouble && triplet.dstAttr._1 < 30000000L ||
              triplet.srcAttr._1 < 30000000L.toDouble && triplet.dstAttr._1 < 20000000L){
              defalutValue = triplet.srcAttr._1 / triplet.toString()(2)
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
        if(id < 20000000L) (attr._1 / author, attr._2)
        else attr
      })

      val paper = hitsGraph.vertices.filter{
        case(id, (key, value)) => if(id > 20000000L && id < 30000000L) true else false}.map( x => {
        ("2", x._2._1)
      }).reduceByKey(_+_).collect()(0)._2
      hitsGraph = hitsGraph.mapVertices( (id, attr) => {
        if(id < 30000000L && id > 20000000L) (attr._1 / paper, attr._2)
        else attr
      })

      val org = hitsGraph.vertices.filter{
        case(id, (key, value)) => if(id > 30000000L && id < 40000000L) true else false}.map( x => {
        ("3", x._2._1)
      }).reduceByKey(_+_).collect()(0)._2
      hitsGraph = hitsGraph.mapVertices( (id, attr) => {
        if(id < 40000000L && id > 30000000L) (attr._1 / org, attr._2)
        else attr
      })

      val venue = hitsGraph.vertices.filter{
        case(id, (key, value)) => if(id > 40000000L) true else false}.map( x => {
        ("4", x._2._1)
      }).reduceByKey(_+_).collect()(0)._2
      hitsGraph = hitsGraph.mapVertices( (id, attr) => {
        if(id > 40000000L) (attr._1 / venue, attr._2)
        else attr
      })

      t = t + 1
    }

    hitsGraph.vertices.map( x => {
      (x._1, x._2._1)
    }).saveAsTextFile("hdfs://hadoop-server:9000/hi_index")
    hitsGraph
  }
}
