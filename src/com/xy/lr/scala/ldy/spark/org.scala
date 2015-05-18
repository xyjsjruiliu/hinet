package com.xy.lr.scala.ldy.spark

/**
 * Created by xylr on 15-5-18.
 */
class org {
  private var oname : String = _
  private var otype : String = _

  def this(s : String){
    this()
  }

  def this(oname : String, otype : String){
    this()
    this.oname = oname
    this.otype = otype
  }

  def setOname(oname : String): Unit ={
    this.oname = oname
  }

  def getOname : String = {
    oname
  }

  def setOtype(otype : String): Unit ={
    this.otype = otype
  }

  def getOtype : String = {
    otype
  }

}
