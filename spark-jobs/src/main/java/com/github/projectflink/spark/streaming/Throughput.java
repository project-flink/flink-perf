package com.github.projectflink.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Seconds;
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

/**
 * Created by robert on 6/26/15.
 */
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
						store(new Tuple4<>(id++,streamId(), 0L, payload));
					}
				}
			});

			runner.start();
		}

		@Override
		public void onStop() {
			running = false;
			runner.yield();
		}
	}

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("throughput").setMaster("local[3]");
		JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(10000));

		JavaReceiverInputDStream<Tuple4<Long, Integer, Long, byte[]>> source = ssc.receiverStream(new Source(StorageLevel.MEMORY_AND_DISK_2()));
		JavaPairDStream<Long, Tuple3<Integer, Long, byte[]>> kvsource = source.mapToPair(new PairFunction<Tuple4<Long, Integer, Long, byte[]>, Long, Tuple3<Integer, Long, byte[]>>() {
			@Override
			public Tuple2<Long, Tuple3<Integer, Long, byte[]>> call(Tuple4<Long, Integer, Long, byte[]> longIntegerLongTuple4) throws Exception {
				return new Tuple2<Long, Tuple3<Integer, Long, byte[]>>(longIntegerLongTuple4._1(),
						new Tuple3<Integer, Long, byte[]>(longIntegerLongTuple4._2(), longIntegerLongTuple4._3(), longIntegerLongTuple4._4()));
			}
		});
		JavaDStream<Integer> res = kvsource.repartition(3).map(new Function<Tuple2<Long, Tuple3<Integer, Long, byte[]>>, Integer>() {
			long received = 0;
			long start = 0;
			long logfreq = 100000;

			@Override
			public Integer call(Tuple2<Long, Tuple3<Integer, Long, byte[]>> v1) throws Exception {

				if (start == 0) {
					start = System.currentTimeMillis();
				}
				received++;
				if (received % logfreq == 0) {
					long sinceSec = ((System.currentTimeMillis() - start) / 1000);
					if (sinceSec == 0) return 0;
					LOG.info("Received {} elements since {}. Elements per second {}, GB received {}",
							received,
							sinceSec,
							received / sinceSec,
							(received * (8 + 4 + 12)) / 1024 / 1024 / 1024);
				}
				if (v1._2()._2() != 0) {
					long lat = System.currentTimeMillis() - v1._2()._2();
					LOG.info("Latency {} ms", lat);
				}

				return null;
			}
		});

		res.print();

		ssc.start();
	}
}
