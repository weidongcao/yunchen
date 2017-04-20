package com.rainsoft.spark.scala

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017-04-12.
  */
object SparkMLlibTest {

    val filepath: String = "D:\\Program Files\\Java\\JetBrains\\workspace\\bigdata\\src\\main\\zdata\\bcp\\testdata"

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setAppName("Spark MLlib test")
            .setMaster("local")

        val sc = new SparkContext(conf)

        val originalRDD = sc.textFile("file:///" + filepath)

        val mllibRDD = originalRDD.filter("".equals(_) == false)
            .map(line => Vectors.dense(line.split(" ").map(_.trim.toDouble)))
            .cache()
        //            .collect()
        //            .foreach(println(_))

        //设置簇的个数为3
        val numClusters = 3

        //迭代20次
        val numIterations = 20

        //运行10次，选出最优解
        val runs = 10

        //调协初始K选取方式为K-means++
        val initMode = "k-means||"

        val clusters = new KMeans()
            .setInitializationMode(initMode)
            .setK(numClusters)
            .setMaxIterations(numIterations)
            .run(mllibRDD)

        println(mllibRDD.map(line => line.toString + "belong to cluster: " + clusters.predict(line)).collect().mkString("\n"))

        val wcsse = clusters.computeCost(mllibRDD)

        println("withinSet sum of squared Error = " + wcsse)

        val a21 = clusters.predict(Vectors.dense(1.2, 1.3))

        val a22 = clusters.predict(Vectors.dense(4.1, 4.2))

        println("Culstercenters")

        clusters.clusterCenters.foreach(println(_))

        println("Prediction of (1.2, 1,3) " + a21)
        println("prediction of (4.1, 4.2) " + a22)
    }
}
