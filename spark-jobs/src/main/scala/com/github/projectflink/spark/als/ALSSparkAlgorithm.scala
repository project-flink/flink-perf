package com.github.projectflink.spark.als

import com.github.projectflink.common.als.{Factors, ALSAlgorithm}

import scala.util.Random

trait ALSSparkAlgorithm extends ALSSpark with ALSAlgorithm{
  def generateRandomMatrix(users: DS[IDType], factors: Int, seed: Long): DS[FactorType] = {
    users map {
      id =>{
        val random = new Random(id ^ seed)
        new Factors(id, randomFactors(factors, random))
      }
    }
  }

  def randomFactors(factors: Int, random: Random): Array[ElementType] ={
    Array.fill(factors)(random.nextDouble().asInstanceOf[ElementType])
  }
}
