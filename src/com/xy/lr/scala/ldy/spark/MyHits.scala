package com.xy.lr.scala.ldy.spark

import org.apache.spark.graphx._

/**
 * Created by xylr on 15-5-19.
 */
object MyHits{

  def run(graph : Graph[String, String], maxIterations: Int = Int.MaxValue): Graph[(Double, String), String] ={

    //initial the graph by set first 1.0 value
    val initialGraph : Graph[(Double/*hits value*/ , String), String] = graph.mapVertices(
      (id, attr) =>{
        (1.0, attr)
      }
    )

    val hitsGraph = initialGraph.pregel(
      //initialMsg the message each vertex will receive at the on the first iteration
      initialMsg = (1.0, ""),
      //the maximum number of iterations
      maxIterations,
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
    hitsGraph
  }
}
