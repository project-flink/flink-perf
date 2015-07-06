package com.github.projectflink.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.util.Iterator;

public class Throughput {

	private static final Logger LOG = LoggerFactory.getLogger(Throughput.class);

	public static class Source extends Receiver<Tuple4<Long, Integer, Long, byte[]>> {

		transient Thread runner;
		boolean running = true;
		final byte[] payload;
		public Source(StorageLevel storageLevel) {
			super(storageLevel);
			payload = new byte[12];
		}

		@Override
		public StorageLevel storageLevel() {
			return StorageLevel.MEMORY_AND_DISK_2();
		}

		@Override
		public void onStart() {

			runner = new Thread(new Runnable() {

				@Override
				public void run() {
					long id = 0;
					while(running) {
						store(new Tuple4<>(id++, streamId(), 0L, payload));
					}
					System.out.println("Generated "+id+" records");
				}
			});

			runner.start();
		}

		@Override
		public void onStop(){
			System.out.println("++++ STOPPING RECEIVER ++++");
			running = false;
			try {
				runner.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("throughput").setMaster("local[16]");
		JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(1000));

		JavaReceiverInputDStream<Tuple4<Long, Integer, Long, byte[]>> source = ssc.receiverStream(new Source(StorageLevel.MEMORY_AND_DISK_2()));
		JavaPairDStream<Long, Tuple3<Integer, Long, byte[]>> kvsource = source.mapToPair(new PairFunction<Tuple4<Long, Integer, Long, byte[]>, Long, Tuple3<Integer, Long, byte[]>>() {
			@Override
			public Tuple2<Long, Tuple3<Integer, Long, byte[]>> call(Tuple4<Long, Integer, Long, byte[]> longIntegerLongTuple4) throws Exception {
				return new Tuple2<Long, Tuple3<Integer, Long, byte[]>>(longIntegerLongTuple4._1(),
						new Tuple3<Integer, Long, byte[]>(longIntegerLongTuple4._2(), longIntegerLongTuple4._3(), longIntegerLongTuple4._4()));
			}
		});
		JavaDStream<Long> res = kvsource.repartition(3).mapPartitions(new FlatMapFunction<Iterator<Tuple2<Long,Tuple3<Integer,Long,byte[]>>>, Long>() {
			@Override
			public Iterable<Long> call(Iterator<Tuple2<Long, Tuple3<Integer, Long, byte[]>>> tuple2Iterator) throws Exception {
				long start = System.currentTimeMillis();
				long received = 0;
				while(tuple2Iterator.hasNext()) {
					received++;
					Tuple2<Long, Tuple3<Integer, Long, byte[]>> el = tuple2Iterator.next();

					if (el._2()._2() != 0) {
						long lat = System.currentTimeMillis() - el._2()._2();
						System.out.println("Latency " + lat + " ms");
					}
				}
				long sinceMs = (System.currentTimeMillis() - start);

				System.out.println("Finished Batch. Processed "+received+" elements in "+sinceMs+" ms.");

				return new Iterable<Long>() {
					@Override
					public Iterator<Long> iterator() {
						return new Iterator<Long>() {
							@Override
							public boolean hasNext() {
								return false;
							}

							@Override
							public Long next() {
								return 1L;
							}

							@Override
							public void remove() {

							}
						};
					}
				};
			}


		/*	@Override
			public Long call(Tuple2<Long, Tuple3<Integer, Long, byte[]>> v1) throws Exception {
				//	System.out.println("Recevied " + v1);
				if (start == 0) {

				}
				received++;
				if (received % logfreq == 0) {

					if (sinceSec == 0) {
						System.out.println("received " + received + " elements since 0");
						return 0L;
					}
					System.out.println("Received " + received + " elements since " + sinceSec + ". " +
							"Elements per second " + received / sinceSec + ", GB received " + ((received * (8 + 4 + 12)) / 1024 / 1024 / 1024));
				}
				if (v1._2()._2() != 0) {
					long lat = System.currentTimeMillis() - v1._2()._2();
					System.out.println("Latency " + lat + " ms");
				}

				return received;
			} */
		});


		//res.print();
		/*res.foreachRDD(new Function2<JavaRDD<Long>, Time, Void>() {
			@Override
			public Void call(JavaRDD<Long> integerJavaRDD, Time t) throws Exception {
				integerJavaRDD.saveAsTextFile("/home/robert/flink-workdir/flink-perf/out/"+t.toString());
				return null;
			}
		}); */
		res.print();
	//	res.print();

		ssc.start();
	}
}
