package com.github.projectflink.common.als

case class Rating[I, T](user: I, item: I, rating: T)
case class Factors[I, T](id: I, factors: Array[T]){
  override def toString = s"($id, ${factors.mkString(",")})"
}

trait ALS {
  type DS[T]
  type ElementType
  type IDType
  type FactorType = Factors[IDType, ElementType]
  type RatingType = Rating[IDType, ElementType]

  case class Factorization(userFactors: DS[FactorType], itemFactors: DS[FactorType])
}
