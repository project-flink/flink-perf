package com.github.projectflink.als


import com.github.projectflink.util.FlinkTools
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
import scala.util.{Random, Sorting}

class ALSJoinBlocking(dop: Int, factors: Int, lambda: Double, iterations: Int, userBlocks: Int,
                      itemBlocks: Int, seed: Long, persistencePath: Option[String]) extends
ALSFlinkAlgorithm with Serializable {
  import ALSJoinBlocking._

  override def factorize(ratings: DS[RatingType]): Factorization = {
    null
  }

  def blockedFactorize(ratings: DS[RatingType]): BlockedFactorization = {
    val ratingsByUserBlock = ratings map {
      rating => {
        val blockID = rating.user % userBlocks
        (blockID, rating)
      }
    }

    val ratingsByItemBlock = ratings map {
      rating => {
        val blockID = rating.item % itemBlocks
        (blockID, new Rating(rating.item, rating.user, rating.rating))
      }
    }

    val blockIDPartitioner = new BlockIDPartitioner()

    val (userIn, userOut, itemIn, itemOut, initialItems) = {
      val (userIn, userOut) = createBlockInformation(userBlocks, itemBlocks, ratingsByUserBlock)
      val (itemIn, itemOut) = createBlockInformation(itemBlocks, userBlocks, ratingsByItemBlock)

      val initialItems = itemOut map {
        outInfos =>{
          val blockID = outInfos._1
          val random = new Random(blockID ^ seed)
          val infos = outInfos._2

          (blockID, infos.elementIDs.map(_ => randomFactors(factors, random)))
        }
      }

      persistencePath match {
        case Some(path) =>
          val (path1, path2) = if(path.endsWith("/")) (path + "user/", path + "item/") else (path
            +"/user/", path+"/item/")
          val (uIn, uOut) = FlinkTools.persist(userIn, userOut, path1)
          val (iIn, iOut, ii) = FlinkTools.persist(itemIn, itemOut, initialItems,
            path2)
          (uIn, uOut, iIn, iOut, ii)
        case None => (userIn, userOut, itemIn, itemOut, initialItems)
      }
    }

    val pInitialItems = initialItems.partitionCustom(blockIDPartitioner, 0)

    val items = pInitialItems.iterate(iterations){
      items => {
        val users = updateFactors(userBlocks, items, itemOut, userIn, factors, lambda,
          blockIDPartitioner)
        updateFactors(itemBlocks, users, userOut, itemIn, factors, lambda, blockIDPartitioner)
      }
    }

//    val users = updateFactors(userBlocks, items, itemOut, userIn, factors, lambda)
//    new Factorization(unblock(users, userOut), unblock(items, itemOut))

    new BlockedFactorization(items, items)
  }

  def unblock(users: DS[(IDType, Array[Array[ElementType]])], outInfo: DS[(IDType, OutBlockInformation
    )]):DS[FactorType] = {
    users.join(outInfo).where(0).equalTo(0){
      (left, right, col: Collector[FactorType]) => {
        val outInfo = right._2
        val factors = left._2
        outInfo.elementIDs.zip(factors).foreach{ case (id, factors) => col.collect(new Factors(id,
          factors))}
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
      (left, right, col: Collector[(IDType, (IDType, Array[Array[ElementType]]))]) => {
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
            if(buf.nonEmpty) {
              col.collect((idx, (blockID, buf.toArray)))
            }
        }
      }
    }

    val blockMsgs =
      if (numUserBlocks == dop) {
       partialBlockMsgs.partitionCustom(blockIDPartitioner, 0)
        .mapPartition {
         partialMsgs => {
            val array = partialMsgs.toArray
            val id = array(0)._1
            val blocks = array.map(_._2)
            List((id, blocks))
          }
        }.withConstantSet("0")
      }else{
        partialBlockMsgs.groupBy(0).withPartitioner(blockIDPartitioner).reduceGroup {
          it => {
            val array = it.toArray
            val id = array(0)._1
            val blocks = array.map(_._2)
            (id, blocks)
          }
        }.withConstantSet("0")
      }

    blockMsgs.join(userIn).where(0).equalTo(0).withPartitioner(blockIDPartitioner).apply{
      (left, right) => {
        val blockID = left._1
        val updates = left._2
        val inInfo = right._2
        (blockID, updateBlock(blockID, updates, inInfo, factors, lambda))
      }
    }.withConstantSetFirst("0")
  }

  def updateBlock(blockID: Int, updates: Array[(IDType, Array[Array[ElementType]])],
                  inInfo: InBlockInformation, factors: Int, lambda: Double):
  Array[Array[ElementType]]
  = {
    val blockFactors = updates.sortBy(_._1).map(_._2).toArray
    val numItemBlocks = blockFactors.length
    val numUsers = inInfo.elementIDs.length

    val userXtX = Array.fill(numUsers)(DenseMatrix.zeros[ElementType](factors, factors))
    val userXy = Array.fill(numUsers)(DenseVector.zeros[ElementType](factors))

    val numRatings = Array.fill(numUsers)(0)

    for(itemBlock <- 0 until numItemBlocks){
      var p = 0
      while(p < blockFactors(itemBlock).length){
        import outerProduct._
        val vector = DenseVector(blockFactors(itemBlock)(p))
        val matrix = outerProduct(vector, vector)
        val (us, rs) = inInfo.ratingsForBlock(itemBlock)(p)

        var i = 0
        while(i < us.length){
          numRatings(us(i)) += 1
          userXtX(us(i)) += matrix
          userXy(us(i)) += vector * rs(i)
          i += 1
        }
        p += 1
      }
    }

    Array.range(0, numUsers) map { index =>
      val matrix = userXtX(index)
      val vector = userXy(index)

      diag(matrix) += lambda.asInstanceOf[ElementType]*numRatings(index)

      (matrix \ vector).data
    }
  }

  def createBlockInformation(userBlocks: Int, itemBlocks: Int, ratings: DS[(IDType,
    RatingType)]):(DS[(IDType, InBlockInformation)],
    DS[(IDType, OutBlockInformation)]) = {
    val blockInformation = ratings.
      groupBy(0).
      reduceGroup{
      ratings => {
        val partitioner = new Partitioner(itemBlocks)
        val arrayRatings = ratings.toArray
        val inBlockInformation = createInBlockInformation(itemBlocks, arrayRatings,
          partitioner)
        val outBlockInformation = createOutBlockInformation(itemBlocks, arrayRatings,
        partitioner)
        (inBlockInformation, outBlockInformation)
      }
    }
    (blockInformation.map(_._1), blockInformation.map(_._2))
  }

  def createInBlockInformation(numItemBlocks: Int, ratings: Array[(IDType, RatingType)],
                               itemPartitioner: Partitioner):
  (IDType, InBlockInformation) = {
    val userIDs = ratings.map(_._2.user).distinct.sorted
    val userIDToPos = userIDs.zipWithIndex.toMap

    val t = new ArrayBuffer[Int]()

    val blockRatings = Array.fill(numItemBlocks)(new ArrayBuffer[RatingType])
    for(r <- ratings) {
      blockRatings(itemPartitioner(r._2.item)) += r._2
    }

    val ratingsForBlock = new Array[BlockRating](numItemBlocks)
    var blockCounter = 0

    for(itemBlock <- 0 until numItemBlocks){
      val groupedRatings = blockRatings(itemBlock).groupBy(_.item).toArray
      val ordering = new Ordering[(IDType, ArrayBuffer[RatingType])] {
        def compare(a: (IDType, ArrayBuffer[RatingType]), b: (IDType,
          ArrayBuffer[RatingType])):Int = {
          (a._1 - b._1)
        }
      }
      Sorting.quickSort(groupedRatings)(ordering)

      if(groupedRatings.nonEmpty){
        ratingsForBlock(blockCounter) = new BlockRating(groupedRatings map {
          case (p, rs) => {
            (rs.view.map(r => userIDToPos(r.user)).toArray, rs.view.map(_.rating).toArray)
          }
        })
        blockCounter += 1
      }
    }

    (ratings(0)._1, InBlockInformation(userIDs, ratingsForBlock))
  }

  def createOutBlockInformation(numItemBlocks: Int, ratings: Array[(IDType, RatingType)],
                                itemPartitioner: Partitioner):
  (IDType, OutBlockInformation) = {
    val userIDs = ratings.map(_._2.user).distinct.sorted
    val numUsers = userIDs.length
    val userIDToPos = userIDs.zipWithIndex.toMap
    val shouldSend = Array.fill(numUsers)(new scala.collection.mutable.BitSet(numItemBlocks))

    for(r <- ratings) {
      shouldSend(userIDToPos(r._2.user))(itemPartitioner(r._2.item)) = true
    }

    (ratings(0)._1, OutBlockInformation(userIDs, new OutLinks(shouldSend)))
  }
}

object ALSJoinBlocking extends ALSFlinkRunner with ALSFlinkToyRatings {
  class Partitioner(blocks: Int){
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
