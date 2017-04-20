package com.rainsoft.test.scala

import java.io.File

/**
  * Created by Administrator on 2017-03-27.
  */
object TestFile {
    def main(args: Array[String]): Unit = {
//        val path = "D:\\Program Files\\Java\\JetBrains\\workspace\\bigdata\\out\\artifacts"
//        deleteFolder(path)
        print('j'.toByte)
    }
    /**
      * 删除指定目录下已经处理过的BCP文件
      *
      * @param dir
      */
    def deleteFolder(dir: String) {
        //创建文件对象
        val delfolder = new File(dir);

        //判断此文件对象是否存在且为目录
        if ((delfolder.exists()) && (delfolder.isDirectory)) {
            //获取目录下所有的文件列表，包括目录和文件
            val oldFile = delfolder.listFiles();

            try {
                //遍历所有文件
                for (i <- 0 to oldFile.length - 1) {
                    //子文件
                    val childFile = oldFile(i)

                    //判断子文件是否存在
                    if (childFile.exists()) {

                        //判断子文件是文件还是目录
                        if (childFile.isDirectory) {
                            //目录的话递归删除所有
                            deleteFolder(dir + "/" + childFile.getName)
                        } else if (childFile.isFile) {
                            //文件直接删除
                            childFile.delete()
                        }
                    }
                }
                println("清空文件夹操作成功!")
            }
            catch {
                case e: Exception => println(e.printStackTrace());
                    println("清空文件夹操作出错!")
                    System.exit(-1)
            }
        }

    }
}

