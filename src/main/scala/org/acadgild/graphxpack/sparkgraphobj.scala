package org.acadgild.graphxpack

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object sparkgraphobj {

  def main(args: Array[String]): Unit = {
    
    //Defining the SPARK Configuration Object
    
    val conf = new SparkConf().setAppName("SparkGraph").setMaster("local")
    
    //Defining the SPARK CONTEXT Object
    
    val sc = new SparkContext(conf)
    
    //Defining the vertex details

    val users: RDD[(VertexId, (String, String))] = sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "doctor")), (5L, ("franklin", "professor")), (2L, ("istoica", "professor"))))

    //Defining the edges of the graph with source vertex id , destination vertex id and relationship
    
    val relationships: RDD[Edge[String]] = sc.parallelize(Array(Edge(3L, 7L, "collaborator"), Edge(5L, 3L, "advisor"), Edge(2L, 5L, "colleague"), Edge(5L, 7L, "peer")))

    //Defining the default user for spark graph
    
    val defaultUser = ("John Doe", "Missing")
    
    //defining the spark graph vertex edges and default user

    val graph = Graph(users, relationships, defaultUser)

    // Count all users which are doctor
    
    graph.vertices.filter { case (id, (name, pos)) => pos == "doctor" }.count

    // Count all users which are student
    
    graph.vertices.filter { x => (x._2._2 == "student") }.count

    // Count all the edges where source > destination
    
    graph.edges.filter(e => (e.srcId > e.dstId)).count

    val facts: RDD[String] = graph.triplets.map(triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)
    
    facts.collect.foreach(println(_))

    val inDegrees: VertexRDD[Int] = graph.inDegrees
    
    inDegrees.collect()
    
    sc.stop()

  }

}