package com.github.projectflink.als

import com.github.projectflink.common.als.{Rating, ALSRunner}
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

import scala.reflect.io.Path

trait ALSFlinkRunner extends ALSFlink with ALSRunner {
  type Context = ExecutionEnvironment

  def readRatings(input: String, env: Context): DS[RatingType] = {
    env.readCsvFile[(IDType, IDType, ElementType)](
      input,
      "\n",
      ','
    ).map{ x => Rating[IDType, ElementType](x._1, x._2, x._3) }
  }

  def outputFactorization(factorization: ALSFlink#Factorization, outputPath: String): Unit = {
    if(outputPath == null || outputPath.isEmpty){
      factorization.userFactors.print()
      factorization.itemFactors.print()
    }else{
      val path = if(outputPath.endsWith("/")) outputPath else outputPath +"/"
      val userPath = path + USER_FACTORS_FILE
      val itemPath = path + ITEM_FACTORS_FILE

      factorization.userFactors.writeAsText(
        userPath,
        WriteMode.OVERWRITE
      )

      factorization.itemFactors.writeAsText(
        itemPath,
        WriteMode.OVERWRITE
      )
    }
  }
}
