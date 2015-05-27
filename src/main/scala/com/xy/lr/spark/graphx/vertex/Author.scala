package com.xy.lr.spark.graphx.vertex

/**
 * Created by xylr on 15-5-27.
 */
class Author{
  //private var id : Long = _
  private var name : String = _
  private var author_affi : String = _
  private var paper_count : Int = _
  private var citation_count : Int = _
  private var h_index : Int = _
  private var p_index : Float = _
  private var up_index : Float = _
  private var keyterms : String = _

  def this(s : String){
    this()
  }

  def this(name : String, author_affi : String, paper_count : Int, citation_count : Int,
           h_index : Int, p_index : Float, up_index : Float, keyterms : String){
    this()
    this.name = name
    this.author_affi = author_affi
    this.citation_count = citation_count
    this.paper_count = paper_count
    this.h_index = h_index
    this.p_index = p_index
    this.up_index = up_index
    this.keyterms = keyterms
  }

  def setName(name : String): Unit ={
    this.name = name
  }

  def getName : String = {
    name
  }

  def setAuthor_affi(author_affi : String): Unit ={
    this.author_affi = author_affi
  }

  def getAuthor_affi : String = {
    author_affi
  }

  def setPaper_count(paper_count : Int): Unit ={
    this.paper_count = paper_count
  }

  def getPaper_count: Int ={
    paper_count
  }

  def setcitation_count(citation_count : Int): Unit ={
    this.citation_count = citation_count
  }

  def getcitation_count : Int = {
    citation_count
  }

  def seth_index(h_index : Int): Unit ={
    this.h_index = h_index
  }

  def geth_index : Int = {
    h_index
  }

  def setp_index(p_index : Float): Unit ={
    this.p_index = p_index
  }

  def getp_index : Float = {
    p_index
  }

  def setup_index(up_index : Float): Unit ={
    this.up_index = up_index
  }

  def getup_index : Float = {
    up_index
  }

  def setkeyterms(keyterms : String): Unit ={
    this.keyterms = keyterms
  }

  def getkeyterms : String = {
    keyterms
  }

}
