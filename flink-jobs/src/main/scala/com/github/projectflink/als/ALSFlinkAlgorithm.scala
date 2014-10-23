package com.github.projectflink.als

import com.github.projectflink.common.als.{Factors, ALSAlgorithm}
import org.apache.flink.api.scala._

import scala.util.Random

trait ALSFlinkAlgorithm extends ALSFlink with ALSAlgorithm {
  def generateRandomMatrix(users: DS[IDType], factors: Int, seed: Long): DS[FactorType] = {
    users map {
      id =>{
        val random = new Random(id ^ seed)
        Factors[IDType, ElementType](id, randomFactors(factors, random))
      }
    }
  }

  def randomFactors(factors: Int, random: Random): Array[ElementType] ={
    Array.fill(factors)(random.nextDouble())
  }
}
