package com.github.projectflink.common.als

trait ALSAlgorithm extends ALS {
  def factorize(ratings: DS[RatingType]): Factorization
}
