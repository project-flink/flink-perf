/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.projectflink.als

import java.lang

import com.github.projectflink.util.FlinkTools
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._
import com.github.projectflink.common.als.{ALSUtils, Factors, Rating}
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.core.memory.{DataOutputView, DataInputView}
import org.apache.flink.types.Value
import org.apache.flink.util.Collector
import org.apache.flink.api.common.functions.{Partitioner => FlinkPartitioner,
GroupReduceFunction, CoGroupFunction}
import org.jblas.{Solve, SimpleBlas, FloatMatrix}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.{Random}

/**
 * This ALS implementation is based on Spark's MLLib implementation of ALS [https://github.com/
 * apache/spark/blob/master/mllib/src/main/scala/org/apache/spark/mllib/recommendation/ALS.scala]
 */
class ALSJoinBlocking(dop: Int, factors: Int, lambda: Double, iterations: Int, userBlocks: Int,
                      itemBlocks: Int, seed: Long, persistencePath: Option[String]) extends
ALSFlinkAlgorithm with Serializable {

  import ALSJoinBlocking._

  def factorize(ratings: DS[RatingType]): Factorization = {
    val blockIDPartitioner = new BlockIDPartitioner()

    val ratingsByUserBlock = ratings.map{
      rating => {
        val blockID = rating.user % userBlocks
        (blockID, rating)
      }
    } partitionCustom(blockIDPartitioner, 0)

    val ratingsByItemBlock = ratings map {
      rating => {
        val blockID = rating.item % itemBlocks
        (blockID, new Rating(rating.item, rating.user, rating.rating))
      }
    } partitionCustom(blockIDPartitioner, 0)

    val (uIn, uOut) = createBlockInformation(userBlocks, itemBlocks, ratingsByUserBlock,
      blockIDPartitioner)
    val (iIn, iOut) = createBlockInformation(itemBlocks, userBlocks, ratingsByItemBlock,
      blockIDPartitioner)

    val (userIn, userOut) = persistencePath match {
      case Some(path) => {
        FlinkTools.persist(uIn, uOut, path + "userIn", path + "userOut")
      }
      case None => {
        (uIn, uOut)
      }
    }

    val (itemIn, itemOut) = persistencePath match {
      case Some(path) => {
        FlinkTools.persist(iIn, iOut, path + "itemIn", path + "itemOut")
      }
      case None => {
        (iIn, iOut)
      }
    }

    val initialItems = itemOut.partitionCustom(blockIDPartitioner, 0).map{
      outInfos => {
        val blockID = outInfos._1
        val infos = outInfos._2

        (blockID, infos.elementIDs.map{
          id =>
            val random = new Random(id ^ seed)
            randomFactors(factors, random)
        })
      }
    }.withForwardedFields("0")

    val items = initialItems.iterate(iterations) {
      items => {
        val users = updateFactors(userBlocks, items, itemOut, userIn, factors, lambda,
          blockIDPartitioner)
        updateFactors(itemBlocks, users, userOut, itemIn, factors, lambda, blockIDPartitioner)
      }
    }

    val pItems = persistencePath match {
      case Some(path) => FlinkTools.persist(items, path + "items")
      case None => items
    }

    val users = updateFactors(userBlocks, pItems, itemOut, userIn, factors, lambda,
      blockIDPartitioner)

    new Factorization(unblock(users, userOut, blockIDPartitioner), unblock(pItems, itemOut,
      blockIDPartitioner))
  }

  def unblock(users: DS[(IDType, Array[Array[ElementType]])], outInfo: DS[(IDType, OutBlockInformation
    )], blockIDPartitioner: BlockIDPartitioner): DS[FactorType] = {
    users.join(outInfo).where(0).equalTo(0).withPartitioner(blockIDPartitioner).apply {
      (left, right, col: Collector[FactorType]) => {
        val outInfo = right._2
        val factors = left._2

        for(i <- 0 until outInfo.elementIDs.length){
          val id = outInfo.elementIDs(i)
          val factorVector = factors(i)
          col.collect(Factors(id, factorVector))
        }
      }
    }
  }

  def updateFactors(
                     numUserBlocks: Int,
                     items: DS[(IDType, Array[Array[ElementType]])],
                     itemOut: DS[(IDType, OutBlockInformation)],
                     userIn: DS[(IDType, InBlockInformation)],
                     factors: Int,
                     lambda: Double, blockIDPartitioner: FlinkPartitioner[IDType]):
  DS[(IDType, Array[Array[ElementType]])] = {
    // send the item vectors to the blocks whose users have rated the items
    val partialBlockMsgs = itemOut.join(items).where(0).equalTo(0).
      withPartitioner(blockIDPartitioner).apply {
      (left, right, col: Collector[(IDType, IDType, Array[Array[ElementType]])]) => {
        val blockID = left._1
        val outInfo = left._2
        val factors = right._2
        // array which stores for every user block the item vectors it will receive
        var userBlock = 0
        var item = 0

        while(userBlock < numUserBlocks){
          item = 0
          val buffer = new ArrayBuffer[Array[ElementType]]
          while(item < outInfo.elementIDs.length){
            if(outInfo.outLinks(userBlock)(item)){
              buffer += factors(item)
            }
            item += 1
          }

          if(buffer.nonEmpty){
            col.collect(userBlock, blockID, buffer.toArray)
          }

          userBlock += 1
        }
      }
    }

    partialBlockMsgs.coGroup(userIn).where(0).equalTo(0).sortFirstGroup(1, Order.ASCENDING).
      withPartitioner(blockIDPartitioner).apply{
          new CoGroupFunction[(IDType, IDType, Array[Array[ElementType]]), (IDType,
            InBlockInformation), (IDType, Array[Array[ElementType]])](){

            val triangleSize = (factors*factors - factors)/2 + factors
            val matrix = FloatMatrix.zeros(triangleSize)
            val fullMatrix = FloatMatrix.zeros(factors, factors)
            val userXtX = new ArrayBuffer[FloatMatrix]()
            val userXy = new ArrayBuffer[FloatMatrix]()
            val numRatings = new ArrayBuffer[Int]()

            override def coGroup(left: lang.Iterable[(IDType, IDType,
              Array[Array[ElementType]])], right: lang.Iterable[(IDType, InBlockInformation)
              ], collector: Collector[(IDType, Array[Array[ElementType]])]): Unit = {
              // there is only one InBlockInformation per user block
              val inInfo = right.iterator().next()._2
              val updates = left.iterator()

              val numUsers = inInfo.elementIDs.length
              var blockID = -1

              var i = 0

//              val userXtX = new Array[FloatMatrix](numUsers)
//              val userXy = new Array[FloatMatrix](numUsers)
//              val numRatings = new Array[Int](numUsers)
//
//              while(i < numUsers){
//                userXtX(i) = FloatMatrix.zeros(triangleSize)
//                userXy(i) = FloatMatrix.zeros(factors)
//
//                i += 1
//              }

              val matricesToClear = if(numUsers > userXtX.length){
                val oldLength = userXtX.length
                while(i < (numUsers - oldLength)) {
                  userXtX += FloatMatrix.zeros(triangleSize)
                  userXy += FloatMatrix.zeros(factors)
                  numRatings.+=(0)

                  i += 1
                }

                oldLength
              }else{
                numUsers
              }

              i = 0
              while(i  < matricesToClear){
                numRatings(i) = 0
                userXtX(i).fill(0.0f)
                userXy(i).fill(0.0f)

                i += 1
              }

              var itemBlock = 0

              while(updates.hasNext){
                val update = updates.next()
                val blockFactors = update._3
                blockID = update._1

                var p = 0
                while(p < blockFactors.length){
                  val vector = new FloatMatrix(blockFactors(p))
                  ALSUtils.outerProduct(vector, matrix, factors)

                  val (users, ratings) = inInfo.ratingsForBlock(itemBlock)(p)

                  var i = 0
                  while (i < users.length) {
                    numRatings(users(i)) += 1
                    userXtX(users(i)).addi(matrix)
                    SimpleBlas.axpy(ratings(i), vector, userXy(users(i)))

                    i += 1
                  }
                  p += 1
                }

                itemBlock += 1
              }

              val array = new Array[Array[ElementType]](numUsers)

              i = 0
              while(i < numUsers){
                ALSUtils.generateFullMatrix(userXtX(i), fullMatrix, factors)

                var f = 0
                while(f < factors){
                  fullMatrix.data(f*factors + f) += lambda.asInstanceOf[ElementType] * numRatings(i)
                  f += 1
                }

                array(i) = Solve.solvePositive(fullMatrix, userXy(i)).data

                i += 1
              }

              collector.collect((blockID, array))
            }
          }
    }.withForwardedFieldsFirst("0").withForwardedFieldsSecond("0")
  }

  def createUsersPerBlock(ratings: DS[(IDType, RatingType)]):
  DS[(IDType, Array[IDType])] = {
    //    ratings.map { x => (x._1, x._2.user)}.withForwardedFields("0").distinct(0, 1).
    //      groupBy(0).sortGroup(1, Order.ASCENDING).reduceGroup {
    //          new RichGroupReduceFunction[(IDType, IDType), (IDType, Array[IDType])] {
    //            override def reduce(iterable: lang.Iterable[(IDType, IDType)], collector: Collector[
    //              (IDType, Array[IDType])]): Unit = {
    //              import scala.collection.JavaConverters._
    //              val users = iterable.iterator().asScala
    //
    //              val bufferedUsers = users.buffered
    //              val head = bufferedUsers.head
    //              val id = bufferedUsers.head._1
    //              val userIDs = bufferedUsers.map { x => x._2}.toArray
    //
    //              if(this.getRuntimeContext.getIndexOfThisSubtask==0) {
    //                println(s"ID:$id [Users:${userIDs.mkString(", ")}] head: $head")
    //              }
    //              collector.collect((id, userIDs))
    //            }
    //          }
    //    }.withForwardedFields("0")

    ratings.map{ x => (x._1, x._2.user)}.withForwardedFields("0").
      groupBy(0).sortGroup(1, Order.ASCENDING).reduceGroup {
      users => {
        val result = ArrayBuffer[Int]()
        var id = -1
        var oldUser = -1

        while(users.hasNext) {
          val user = users.next()

          id = user._1

          if (user._2 != oldUser) {
            result.+=(user._2)
            oldUser = user._2
          }
        }

        val userIDs = result.toArray
        (id, userIDs)
      }
    }.withForwardedFields("0")
  }

  /**
   * Creates for every user block the out-going block information. The out block information
   * contains for every item block a bitset which indicates which user vector has to be sent to
   * this block. If a vector v has to be sent to a block b, then bitsets(b)'s bit v is
   * set to 1, otherwise 0. Additionally the user IDs are replaced by the user vector's index value.
   *
   * @param ratings
   * @param usersPerBlock
   * @param itemBlocks
   * @param partitioner
   * @return
   */
  def createOutBlockInformation(ratings: DS[(IDType, RatingType)], usersPerBlock: DS[
    (IDType, Array[IDType])], itemBlocks: Int, partitioner: Partitioner): DS[(IDType, OutBlockInformation)]
  = {
    ratings.coGroup(usersPerBlock).where(0).equalTo(0).apply {
      (ratings, users) =>
        val userIDs = users.next()._2
        val numUsers = userIDs.length

        val userIDToPos = userIDs.zipWithIndex.toMap

        val shouldSend = Array.fill(itemBlocks)(new scala.collection.mutable.BitSet(numUsers))
        var blockID = -1
        while (ratings.hasNext) {
          val r = ratings.next

          val pos =
            try {
              userIDToPos(r._2.user)
            }catch{
              case e: NoSuchElementException => throw new RuntimeException(s"Key ${r._2.user} not" +
                s" found. BlockID ${blockID}. Elements in block ${userIDs.take(5).mkString(", ")}" +
                s". UserIDList contains ${userIDs.contains(r._2.user)}.",
                e)
            }

          blockID = r._1
          shouldSend(partitioner(r._2.item))(pos) = true
        }

        (blockID, OutBlockInformation(userIDs, new OutLinks(shouldSend)))
    }.withForwardedFieldsFirst("0").withForwardedFieldsSecond("0")
  }

  /**
   * Creates for every user block the incoming block information. The incoming block information
   * contains the userIDs of the users in the respective block and for every item block a
   * BlockRating instance. The BlockRating instance describes for every incoming set of item
   * vectors of an item block, which user rated these items and what the rating was. For that
   * purpose it contains for every incoming item vector a tuple of an id array us and a rating
   * array rs. The array us contains the indices of the users having rated the respective
   * item vector with the ratings in rs.
   *
   * @param ratings
   * @param usersPerBlock
   * @param partitioner
   * @return
   */
  def createInBlockInformation(ratings: DS[(IDType, RatingType)], usersPerBlock: DS[(IDType,
    Array[IDType])], partitioner: Partitioner): DS[(IDType, InBlockInformation)] = {
    // Group for every user block the users which have rated the same item and collect their ratings
    val partialInInfos = ratings.map { x => (x._1, x._2.item, x._2.user, x._2.rating)}
      .withForwardedFields("0").groupBy(0, 1).reduceGroup {
      x =>
        var userBlockID = -1
        var itemID = -1
        val userIDs = ArrayBuffer[IDType]()
        val ratings = ArrayBuffer[ElementType]()
        while (x.hasNext) {
          val (uBlockID, item, user, rating) = x.next
          userBlockID = uBlockID
          itemID = item

          userIDs += user
          ratings += rating
        }

        (userBlockID, partitioner(itemID), itemID, (userIDs.toArray, ratings.toArray))
    }.withForwardedFields("0")

    // Aggregate all ratings for items belonging to the same item block. Sort ascending with
    // respect to the itemID, because later the item vectors of the update message are sorted
    // accordingly.
    val collectedPartialInfos = partialInInfos.groupBy(0, 1).sortGroup(2, Order.ASCENDING).
      reduceGroup {
      new GroupReduceFunction[(Int, Int, Int, (Array[IDType], Array[ElementType])), (IDType,
        IDType, Array[(Array[IDType], Array[ElementType])])](){
        val buffer = new ArrayBuffer[(Array[IDType], Array[ElementType])]

        override def reduce(iterable: lang.Iterable[(Int, Int, Int, (Array[IDType],
          Array[ElementType]))], collector: Collector[(IDType, IDType, Array[(Array[IDType],
          Array[ElementType])])]): Unit = {

          val infos = iterable.iterator()
          var counter = 0

          var blockID = -1
          var itemBlockID = -1

          while (infos.hasNext && counter < buffer.length) {
            val info = infos.next()
            blockID = info._1
            itemBlockID = info._2

            buffer(counter) = info._4

            counter += 1
          }

          while (infos.hasNext) {
            val info = infos.next()
            blockID = info._1
            itemBlockID = info._2

            buffer += info._4

            counter += 1
          }

          val array = new Array[(Array[IDType], Array[ElementType])](counter)

          buffer.copyToArray(array)

          collector.collect((blockID, itemBlockID, array))
        }
      }
    }.withForwardedFields("0", "1")

    // Aggregate all item block ratings with respect to their user block ID. Sort the blocks with
    // respect to their itemBlockID, because the block update messages are sorted the same way
    collectedPartialInfos.coGroup(usersPerBlock).where(0).equalTo(0).sortFirstGroup(1,
      Order.ASCENDING).apply{
      new CoGroupFunction[(IDType, IDType, Array[(Array[IDType], Array[ElementType])]),
        (IDType, Array[IDType]), (IDType, InBlockInformation)] {
        val buffer = ArrayBuffer[BlockRating]()

        override def coGroup(partialInfosIterable: lang.Iterable[(IDType, IDType, Array[
          (Array[IDType], Array[ElementType])])], userIterable: lang.Iterable[(IDType,
          Array[IDType])], collector: Collector[(IDType, InBlockInformation)]): Unit = {

          val users = userIterable.iterator()
          val partialInfos = partialInfosIterable.iterator()

          val userWrapper = users.next()
          val id = userWrapper._1
          val userIDs = userWrapper._2
          val userIDToPos = userIDs.zipWithIndex.toMap

          var counter = 0

          while (partialInfos.hasNext && counter < buffer.length) {
            val partialInfo = partialInfos.next()
            // entry contains the ratings and userIDs of a complete item block
            val entry = partialInfo._3

            // transform userIDs to positional indices
            for (row <- 0 until entry.length; col <- 0 until entry(row)._1.length) {
              entry(row)._1(col) = userIDToPos(entry(row)._1(col))
            }

            buffer(counter).ratings = entry

            counter += 1
          }

          while (partialInfos.hasNext) {
            val partialInfo = partialInfos.next()
            // entry contains the ratings and userIDs of a complete item block
            val entry = partialInfo._3

            // transform userIDs to positional indices
            for (row <- 0 until entry.length; col <- 0 until entry(row)._1.length) {
              entry(row)._1(col) = userIDToPos(entry(row)._1(col))
            }

            buffer += new BlockRating(entry)

            counter += 1
          }

          val array = new Array[BlockRating](counter)

          buffer.copyToArray(array)

          collector.collect((id, InBlockInformation(userIDs, array)))
        }
      }
    }.withForwardedFieldsFirst("0").withForwardedFieldsSecond("0")
  }

  def createBlockInformation(userBlocks: Int, itemBlocks: Int, ratings: DS[(IDType,
    RatingType)], blockIDPartitioner: BlockIDPartitioner): (DS[(IDType, InBlockInformation)],
    DS[(IDType, OutBlockInformation)]) = {
    val partitioner = new Partitioner(itemBlocks)

    val usersPerBlock = createUsersPerBlock(ratings)

    val outBlockInfos = createOutBlockInformation(ratings, usersPerBlock, itemBlocks, partitioner)

    val inBlockInfos = createInBlockInformation(ratings, usersPerBlock, partitioner)

    (inBlockInfos, outBlockInfos)
  }
}

object ALSJoinBlocking extends ALSFlinkRunner with ALSFlinkToyRatings {
  class Partitioner(blocks: Int) extends Serializable{
    def apply(id: Int): Int = {
      id % blocks
    }
  }

  case class OutBlockInformation(elementIDs: Array[IDType], outLinks: OutLinks){
    override def toString: String = {
      s"OutBlockInformation:((${elementIDs.mkString(",")}), ($outLinks))"
    }
  }

  class OutLinks(var links: Array[scala.collection.mutable.BitSet]) extends Value {
    def this() = this(null)

    override def toString:String = {
      s"${links.mkString("\n")}"
    }

    override def write(out: DataOutputView): Unit = {
      out.writeInt(links.length)
      links foreach {
        link => {
          val bitMask = link.toBitMask
          out.writeInt(bitMask.length)
          for(element <- bitMask){
            out.writeLong(element)
          }
        }
      }
    }

    override def read(in: DataInputView): Unit = {
      val length = in.readInt()
      links = new Array[scala.collection.mutable.BitSet](length)

      for(i <- 0 until length){
        val bitMaskLength = in.readInt()
        val bitMask = new Array[Long](bitMaskLength)
        for(j <- 0 until bitMaskLength){
          bitMask(j) = in.readLong()
        }
        links(i) = mutable.BitSet.fromBitMask(bitMask)
      }
    }

    def apply(idx: Int) = links(idx)
  }

  case class InBlockInformation(elementIDs: Array[IDType], ratingsForBlock: Array[BlockRating]){
    override def toString: String = {
      s"InBlockInformation:((${elementIDs.mkString(",")}), (${ratingsForBlock.mkString("\n")}))"
    }
  }

  case class BlockRating(var ratings: Array[(Array[IDType], Array[ElementType])]) {
    def apply(idx: Int) = ratings(idx)

    override def toString:String = {
      ratings.map{
        case (left, right) => s"((${left.mkString(",")}),(${right.mkString(",")}))"
      }.mkString(",")
    }
  }

  def main(args: Array[String]): Unit = {
    parseCL(args) map {
      config => {
        import config._

        val env = ExecutionEnvironment.getExecutionEnvironment

        val ratings = readRatings(inputRatings, env)

        val numBlocks = if(blocks <= 0){
          env.getParallelism
        }else{
          blocks
        }

        val als = new ALSJoinBlocking(env.getParallelism, factors, lambda, iterations,
          numBlocks, numBlocks, seed, persistencePath)

        val blockFactorization = als.factorize(ratings)

        outputFactorization(blockFactorization, outputPath)

        env.execute("ALSJoinBlocking")
//        println(env.getExecutionPlan())
      }
    } getOrElse{
      println("Could not parse command line arguments.")
    }
  }

  case class BlockedFactorization(userFactors: DS[(IDType, Array[Array[ElementType]])],
                                  itemFactors: DS[(IDType, Array[Array[ElementType]])])

  class BlockIDPartitioner extends FlinkPartitioner[IDType]{
    override def partition(blockID: ALSJoinBlocking.IDType, numberOfPartitions: Int): Int = {
      blockID % numberOfPartitions
    }
  }

  def generateBlockOutput(ds: DataSet[(IDType, Array[Array[ElementType]])]): DS[String] = {
    ds.map {
      block => {
        val factorArrayStr = block._2.map{
          factors =>
            s"(${factors.mkString(", ")})"
        }.mkString("\n")
        s"Block: ${block._1}\n" + factorArrayStr
      }
    }
  }

  def outputBlockedFactorization(factorization: BlockedFactorization, outputPath: String): Unit = {

    val userStrs = generateBlockOutput(factorization.userFactors)
    val itemStrs = generateBlockOutput(factorization.itemFactors)

    if(outputPath == null || outputPath.isEmpty){
      userStrs.print()
      itemStrs.print()
    }else{
      val path = if(outputPath.endsWith("/")) outputPath else outputPath +"/"
      val userPath = path + USER_FACTORS_FILE
      val itemPath = path + ITEM_FACTORS_FILE

      userStrs.writeAsText(
        userPath,
        WriteMode.OVERWRITE
      )

      itemStrs.writeAsText(
        itemPath,
        WriteMode.OVERWRITE
      )
    }
  }
}
