package com.xy.lr.spark.graphx.vertex

/**
 * Created by xylr on 15-5-27.
 */
class Paper {
  private var title : String = _
  private var authors : String = _
  private var affiliations : String = _
  private var year : Int = _
  private var venue : String = _
  private var abstracts : String = _

  def this(s : String){
    this()
  }

  def this(title : String, authors : String, affiliations : String, year : Int,
           venue : String, abstracts : String){
    this()
    this.title = title
    this.authors = authors
    this.affiliations = affiliations
    this.year = year
    this.venue = venue
    this.abstracts = abstracts
  }

  def setTitle(title : String): Unit ={
    this.title = title
  }

  def getTitle : String = {
    title
  }

  def setAuthors(authors : String): Unit ={
    this.authors = authors
  }

  def getAuthors : String = {
    authors
  }

  def setAffiliations(affiliations : String): Unit ={
    this.affiliations = affiliations
  }

  def getAffiliations : String = {
    affiliations
  }

  def setYear(year : Int): Unit ={
    this.year = year
  }

  def getYear : Int = {
    year
  }

  def setVenue(venue : String): Unit ={
    this.venue = venue
  }

  def getVenue : String = {
    venue
  }

  def setAbstracts(abstracts : String): Unit ={
    this.abstracts = abstracts
  }

  def getAbstracts : String = {
    abstracts
  }


}
