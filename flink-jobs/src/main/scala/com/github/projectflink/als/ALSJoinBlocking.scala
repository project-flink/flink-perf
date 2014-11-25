package com.github.projectflink.als


import com.github.projectflink.util.FlinkTools
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._
import breeze.linalg.{diag, DenseVector, DenseMatrix}
import com.github.projectflink.common.als.{Factors, outerProduct, Rating}
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.core.memory.{DataOutputView, DataInputView}
import org.apache.flink.types.Value
import org.apache.flink.util.Collector
import org.apache.flink.api.common.functions.{Partitioner => FlinkPartitioner}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.{Random}

class ALSJoinBlocking(dop: Int, factors: Int, lambda: Double, iterations: Int, userBlocks: Int,
                      itemBlocks: Int, seed: Long, persistencePath: Option[String]) extends
ALSFlinkAlgorithm with Serializable {

  import ALSJoinBlocking._

  override def factorize(ratings: DS[RatingType]): Factorization = {
    null
  }

  def blockedFactorize(ratings: DS[RatingType]): BlockedFactorization = {
    val blockIDPartitioner = new BlockIDPartitioner()

    val ratingsByUserBlock = ratings.map{
      rating => {
        val blockID = rating.user % userBlocks
        (blockID, rating)
      }
    }.partitionCustom(blockIDPartitioner, 0)

    val ratingsByItemBlock = ratings map {
      rating => {
        val blockID = rating.item % itemBlocks
        (blockID, new Rating(rating.item, rating.user, rating.rating))
      }
    } partitionCustom(blockIDPartitioner, 0)

    val (userIn, userOut, itemIn, itemOut, initialItems) = {
      val (userIn, userOut) = createBlockInformation(userBlocks, itemBlocks, ratingsByUserBlock,
        blockIDPartitioner)
      val (itemIn, itemOut) = createBlockInformation(itemBlocks, userBlocks, ratingsByItemBlock,
        blockIDPartitioner)

      val initialItems = itemOut map {
        outInfos => {
          val blockID = outInfos._1
          val random = new Random(blockID ^ seed)
          val infos = outInfos._2

          (blockID, infos.elementIDs.map(_ => randomFactors(factors, random)))
        }
      }

      persistencePath match {
        case Some(path) =>
          val (path1, path2) = if (path.endsWith("/")) (path + "user/", path + "item/")
          else (path
            + "/user/", path + "/item/")
          FlinkTools.persist(userIn, userOut, itemIn, itemOut, initialItems,
            path1)
        case None => (userIn, userOut, itemIn, itemOut, initialItems)
      }
    }

    val pInitialItems = initialItems.partitionCustom(blockIDPartitioner, 0)

    val items = initialItems.iterate(iterations) {
      items => {
        val users = updateFactors(userBlocks, items, itemOut, userIn, factors, lambda,
          blockIDPartitioner)
        updateFactors(itemBlocks, users, userOut, itemIn, factors, lambda, blockIDPartitioner)
      }
    }

//    val users = updateFactors(userBlocks, pInitialItems, itemOut, userIn, factors, lambda,
//      blockIDPartitioner)
//    val items = updateFactors(itemBlocks, users, userOut, itemIn, factors, lambda,
//      blockIDPartitioner)

    //    val users = updateFactors(userBlocks, items, itemOut, userIn, factors, lambda)
    //    new Factorization(unblock(users, userOut), unblock(items, itemOut))

    new BlockedFactorization(items, items)
  }

  def unblock(users: DS[(IDType, Array[Array[ElementType]])], outInfo: DS[(IDType, OutBlockInformation
    )]): DS[FactorType] = {
    users.join(outInfo).where(0).equalTo(0) {
      (left, right, col: Collector[FactorType]) => {
        val outInfo = right._2
        val factors = left._2
        outInfo.elementIDs.zip(factors).foreach { case (id, factors) => col.collect(new Factors(id,
          factors))
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
    val partialBlockMsgs = itemOut.join(items).where(0).equalTo(0).
      withPartitioner(blockIDPartitioner).apply {
      (left, right, col: Collector[(IDType, IDType, Array[Array[ElementType]])]) => {
        val blockID = left._1
        val outInfo = left._2
        val factors = right._2
        val toSend = Array.fill(numUserBlocks)(new ArrayBuffer[Array[ElementType]])
        for (item <- 0 until outInfo.elementIDs.length; userBlock <- 0 until numUserBlocks) {
          if (outInfo.outLinks(item)(userBlock)) {
            toSend(userBlock) += factors(item)
          }
        }
        toSend.zipWithIndex.foreach {
          case (buf, idx) =>
            if (buf.nonEmpty) {
              col.collect((idx, blockID, buf.toArray))
            }
        }
      }
    }

//    val blockMsgs =
//      if (numUserBlocks == dop) {
//        partialBlockMsgs.partitionCustom(blockIDPartitioner, 0)
//          .mapPartition {
//          partialMsgs => {
//            val array = partialMsgs.toArray
//            val id = array(0)._1
//            val blocks = array.map(_._2)
//            List((id, blocks))
//          }
//        }.withConstantSet("0")
//      } else {
//        partialBlockMsgs.groupBy(0).withPartitioner(blockIDPartitioner).reduceGroup {
//          it => {
//            val array = it.toArray
//            val id = array(0)._1
//            val blocks = array.map(_._2)
//            (id, blocks)
//          }
//        }.withConstantSet("0")
//      }

    //partialBlockMsgs.coGroup(userIn).where(0).equalTo(0).withPartitioner(blockIDPartitioner).apply


    partialBlockMsgs.coGroup(userIn).where(0).equalTo(0).sortFirstGroup(1, Order.ASCENDING).
      withPartitioner(blockIDPartitioner).apply{
      (left, right) => {
        val inInfo = right.next()._2
        updateBlock(left, inInfo, factors, lambda)
      }
    }
  }

  def updateBlock(updates: Iterator[(IDType, IDType, Array[Array[ElementType]])],
                  inInfo: InBlockInformation, factors: Int, lambda: Double):(IDType,
    Array[Array[ElementType]]) = {
    val numUsers = inInfo.elementIDs.length
    var blockID = -1

    val userXtX = Array.fill(numUsers)(DenseMatrix.zeros[ElementType](factors, factors))
    val userXy = Array.fill(numUsers)(DenseVector.zeros[ElementType](factors))

    val numRatings = Array.fill(numUsers)(0)

    var itemBlock = 0

    while(updates.hasNext){
      val update = updates.next()
      val blockFactors = update._3
      blockID = update._1

      var p = 0
      while(p < blockFactors.length){
        import outerProduct._
        val vector = DenseVector(blockFactors(p))
        val matrix = outerProduct(vector, vector)
        val (us, rs) = inInfo.ratingsForBlock(itemBlock)(p)

        var i = 0
        while (i < us.length) {
          numRatings(us(i)) += 1
          userXtX(us(i)) += matrix
          userXy(us(i)) += vector * rs(i)
          i += 1
        }
        p += 1
      }

      itemBlock += 1
    }

    (blockID, Array.range(0, numUsers) map { index =>
      val matrix = userXtX(index)
      val vector = userXy(index)

      diag(matrix) += lambda.asInstanceOf[ElementType] * numRatings(index)

      (matrix \ vector).data
    })
  }

  def createBlockInformation(userBlocks: Int, itemBlocks: Int, ratings: DS[(IDType,
    RatingType)], blockIDPartitioner: BlockIDPartitioner): (DS[(IDType, InBlockInformation)],
    DS[(IDType, OutBlockInformation)]) = {
    val users = ratings.map { x => (x._1, x._2.user)}.withConstantSet("0").distinct(0, 1).
      groupBy(0).sortGroup(1, Order.ASCENDING).reduceGroup {
      users => {
        val bufferedUsers = users.buffered
        val id = bufferedUsers.head._1
        val userIDs = bufferedUsers.map { x => x._2}.toArray
        (id, userIDs)
      }
    }.withConstantSet("0")

    val partitioner = new Partitioner(itemBlocks)

    val outBlockInfos = ratings.coGroup(users).where(0).equalTo(0).apply {
      (ratings, users) =>
        val userIDs = users.next()._2
        val numUsers = userIDs.length

        val userIDToPos = userIDs.zipWithIndex.toMap

        val shouldSend = Array.fill(numUsers)(new scala.collection.mutable.BitSet(itemBlocks))
        var blockID = -1
        while (ratings.hasNext) {
          val r = ratings.next

          blockID = r._1
          shouldSend(userIDToPos(r._2.user))(partitioner(r._2.item)) = true
        }

        (blockID, OutBlockInformation(userIDs, new OutLinks(shouldSend)))
    }.withConstantSetFirst("0").withConstantSetSecond("0")

    val partialInInfos = ratings.map { x => (x._1, x._2.user, x._2.item, x._2.rating)}
      .withConstantSet("0").groupBy(0, 2).reduceGroup {
      x =>
        var userBlockID = -1
        var itemID = -1
        val userIDs = ArrayBuffer[IDType]()
        val ratings = ArrayBuffer[ElementType]()
        while (x.hasNext) {
          val (uBlockID, user, item, rating) = x.next
          userBlockID = uBlockID
          itemID = item

          userIDs += user
          ratings += rating
        }

        (userBlockID, partitioner(itemID), itemID, (userIDs.toArray, ratings.toArray))
    }.withConstantSet("0", "2")

    val collectedPartialInfos = partialInInfos.groupBy(0, 1).sortGroup(2, Order.ASCENDING).
      reduceGroup {
      infos => {
        val buffer = new ArrayBuffer[(Array[IDType], Array[ElementType])]()
        var blockID = -1
        var itemBlockID = -1

        while (infos.hasNext) {
          val info = infos.next()
          blockID = info._1
          itemBlockID = info._2

          buffer += info._4
        }

        (blockID, itemBlockID, buffer.toArray)
      }
    }.withConstantSet("0", "1")

    val inBlockInfos = collectedPartialInfos.coGroup(users).where(0).equalTo(0).sortFirstGroup(1,
      Order.ASCENDING).apply{
      (partialInfos, users) => {
        val userWrapper = users.next()
        val id = userWrapper._1
        val userIDs = userWrapper._2
        val userIDToPos = userIDs.zipWithIndex.toMap

        val buffer = ArrayBuffer[BlockRating]()

        while (partialInfos.hasNext) {
          val partialInfo = partialInfos.next()
          val entry = partialInfo._3

          for (row <- 0 until entry.length; col <- 0 until entry(row)._1.length) {
            entry(row)._1(col) = userIDToPos(entry(row)._1(col))
          }

          buffer += new BlockRating(entry)
        }

        (id, InBlockInformation(userIDs, buffer.toArray))
      }
    }.withConstantSetFirst("0").withConstantSetSecond("0")

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

  case class BlockRating(val ratings: Array[(Array[IDType], Array[ElementType])]) {
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
          env.getDegreeOfParallelism
        }else{
          blocks
        }

        val als = new ALSJoinBlocking(env.getDegreeOfParallelism, factors, lambda, iterations,
          numBlocks, numBlocks, seed, persistencePath)

        val blockFactorization = als.blockedFactorize(ratings)

        outputBlockedFactorization(blockFactorization, outputPath)

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
