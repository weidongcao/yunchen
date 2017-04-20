package com.rainsoft.test.scala

import scala.io.Source

/**
  * Created by Administrator on 2017-03-28.
  */
object CompareFile {
    def main(args: Array[String]): Unit = {
        val tomcat = Source.fromFile("D:\\0WorkSpace\\MyResource\\tomcat.txt").getLines()
        val yisou = Source.fromFile("D:\\0WorkSpace\\MyResource\\yisou.txt").getLines()

        for(t <- tomcat) {
            for(y <- yisou) {
                if (t.equals(y)){
                    println(t)
                }
            }
        }
    }

}
