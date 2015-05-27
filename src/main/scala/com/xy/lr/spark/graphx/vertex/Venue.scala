package com.xy.lr.spark.graphx.vertex

/**
 * Created by xylr on 15-5-27.
 */
class Venue {
  private var vname : String = _

  def this(vname : String){
    this()
    this.vname = vname
  }

  def setVname(vname : String): Unit ={
    this.vname = vname
  }

  def getVname : String = {
    vname
  }
}