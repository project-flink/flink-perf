package com.github.projectflink.als


import java.lang

import com.github.projectflink.util.FlinkTools
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._
import com.github.projectflink.common.als.{Factors, Rating}
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.core.memory.{DataOutputView, DataInputView}
import org.apache.flink.types.Value
import org.apache.flink.util.Collector
import org.apache.flink.api.common.functions.{Partitioner => FlinkPartitioner,
GroupReduceFunction, RichGroupReduceFunction, CoGroupFunction}
import org.jblas.{Solve, SimpleBlas, FloatMatrix}

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
          new CoGroupFunction[(IDType, IDType, Array[Array[ElementType]]), (IDType,
            InBlockInformation), (IDType, Array[Array[ElementType]])](){

            val userXtX = ArrayBuffer[FloatMatrix]()
            val userXy = ArrayBuffer[FloatMatrix]()
            val numRatings = ArrayBuffer[Int]()
            val triangleSize = (factors*factors - factors)/2 + factors
            val matrix = FloatMatrix.zeros(triangleSize)
            val fullMatrix = FloatMatrix.zeros(factors, factors)

            override def coGroup(left: lang.Iterable[(IDType, IDType,
              Array[Array[ElementType]])], right: lang.Iterable[(IDType, InBlockInformation)
              ], collector: Collector[(IDType, Array[Array[ElementType]])]): Unit = {
              val inInfo = right.iterator().next()._2
              val updates = left.iterator()

              val numUsers = inInfo.elementIDs.length
              var blockID = -1

              val matricesToClear = if(numUsers > userXtX.length){
                val oldLength = userXtX.length
                var i = 0
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

              var i = 0
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
                  outerProduct(vector, matrix)

                  val (us, rs) = inInfo.ratingsForBlock(itemBlock)(p)

                  var i = 0
                  while (i < us.length) {
                    numRatings(us(i)) += 1
                    userXtX(us(i)).addi(matrix)
                    SimpleBlas.axpy(rs(i), vector, userXy(us(i)))

                    i += 1
                  }
                  p += 1
                }

                itemBlock += 1
              }

              val array = new Array[Array[ElementType]](numUsers)

              i = 0
              while(i < numUsers){
                generateFullMatrix(userXtX(i), fullMatrix)

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
    }.withConstantSetFirst("0").withConstantSetSecond("0")
  }

  def outerProduct(vector: FloatMatrix, matrix: FloatMatrix): Unit = {
    val vd =  vector.data
    val md = matrix.data

    var row = 0
    var pos = 0
    while(row < factors){
      var col = 0
      while(col <= row){
        md(pos) = vd(row) * vd(col)
        col += 1
        pos += 1
      }

      row += 1
    }
  }

  def generateFullMatrix(triangularMatrix: FloatMatrix, fmatrix: FloatMatrix): Unit = {
    var row = 0
    var pos = 0
    val fmd = fmatrix.data
    val tmd = triangularMatrix.data

    while(row < factors){
      var col = 0
      while(col < row){
        fmd(row*factors + col) = tmd(pos)
        fmd(col*factors + row) = tmd(pos)

        pos += 1
        col += 1
      }

      fmd(row*factors + row) = tmd(pos)
      
      pos += 1
      row += 1
    }
  }

  def createBlockInformation(userBlocks: Int, itemBlocks: Int, ratings: DS[(IDType,
    RatingType)], blockIDPartitioner: BlockIDPartitioner): (DS[(IDType, InBlockInformation)],
    DS[(IDType, OutBlockInformation)]) = {
//    val users = ratings.map { x => (x._1, x._2.user)}.withConstantSet("0").distinct(0, 1).
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
//    }.withConstantSet("0")

    val users = ratings.map { x => (x._1, x._2.user)}.withConstantSet("0").
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
//        println(s"ID:$id [Users:${userIDs.mkString(", ")}]")
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
          shouldSend(pos)(partitioner(r._2.item)) = true
        }

        (blockID, OutBlockInformation(userIDs, new OutLinks(shouldSend)))
    }.withConstantSetFirst("0").withConstantSetSecond("0")

    val partialInInfos = ratings.map { x => (x._1, x._2.item, x._2.user, x._2.rating)}
      .withConstantSet("0").groupBy(0, 1).reduceGroup {
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
    }.withConstantSet("0")

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
