package com.rainsoft.test.scala

/**
  * Created by Administrator on 2017-03-31.
  */
class Student(var name: String, var age: Int) {
    private var phone = "170"

    def infoCompObj() = println("伴生类中访问伴生对象： " + Student.sno)
}

object Student {
    private var sno: Int = 100

    def incrementSno() = {
        sno += 1
        sno
    }

    def apply(name: String, age: Int): Student = new Student(name, age)

    def main(args: Array[String]): Unit = {
        println("单例对象" + Student.incrementSno())

        val obj = new Student("CaoWeidong", 27)

        obj.infoCompObj()

        //通过伴生对象的Apply 方法访问伴生类成员
        println("############通过伴生对象的Apply 方法访问伴生类成员#############")
        val obj1 = Student("ZhuXiaomeng", 28)
        println(obj1.name + "--> " + obj1.age)

    }
}
