package com.xy.lr.scala.ldy.spark

/**
 * Created by xylr on 15-5-18.
 */
class venue {
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
