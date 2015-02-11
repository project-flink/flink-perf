package com.github.projectflink.generators.tpch.generators.core;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.Resources;
import io.airlift.tpch.Distribution;
import io.airlift.tpch.Distributions;
import io.airlift.tpch.NationGenerator;
import io.airlift.tpch.PartSupplierGenerator;
import io.airlift.tpch.RegionGenerator;
import io.airlift.tpch.TextPool;
import org.apache.flink.util.SplittableIterator;

import java.lang.reflect.Constructor;
import java.net.URL;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.tpch.DistributionLoader.loadDistribution;

public class TPCHGeneratorSplittableIterator<T> extends SplittableIterator<T> {
	private double scale;
	private int degreeOfParallelism;
	private Class<? extends Iterable<T>> generatorClass;

	public TPCHGeneratorSplittableIterator(double scale, int degreeOfParallelism, Class<? extends Iterable<T>> generatorClass) {
		Preconditions.checkArgument(scale > 0, "Scale must be > 0");
		Preconditions.checkArgument(degreeOfParallelism > 0, "Parallelism must be > 0");

		this.scale = scale;
		this.degreeOfParallelism = degreeOfParallelism;
		this.generatorClass = generatorClass;
	}


	public Iterator<T>[] split(int numPartitions) {
		if(numPartitions > this.degreeOfParallelism) {
			throw new IllegalArgumentException("Too many partitions requested");
		}
		Iterator<T>[] iters = new Iterator[numPartitions];
		for(int i = 1; i <= numPartitions; i++) {
			iters[i - 1] = new TPCHGeneratorSplittableIterator(i, numPartitions, scale, generatorClass);
		}
		return iters;
	}


	public int getMaximumNumberOfSplits() {
		return this.degreeOfParallelism;
	}


	//------------------------ Iterator -----------------------------------
	private static Set<Class<? extends Iterable>> fixedGenerators;

	private static Distributions distributions;
	private static TextPool smallTextPool;
	static {
		fixedGenerators = new HashSet<Class<? extends Iterable>>();
		fixedGenerators.add(RegionGenerator.class);
		fixedGenerators.add(NationGenerator.class);

		try {
			URL resource = Resources.getResource(Distribution.class, "dists.dss");
			checkState(resource != null, "Distribution file 'dists.dss' not found");
			distributions = new Distributions(loadDistribution(Resources.asCharSource(resource, Charsets.UTF_8)));
			smallTextPool = new TextPool(1 * 1024 * 1024, distributions); // 1 MB txt pool
		} catch(Throwable t) {
			throw new RuntimeException("Unable to load distributions", t);
		}
	}

	private Iterator<T> iter;


	public TPCHGeneratorSplittableIterator(int partNo, int totalParts, double scale, Class<? extends Iterable<T>> generatorClass) {
		try {


			Constructor<? extends Iterable<T>> generatorCtor;
			Iterable<T> generator = null;
			if(fixedGenerators.contains(generatorClass)) {
				// use short constructor:
				generatorCtor = generatorClass.getConstructor(Distributions.class, TextPool.class);
				generator = generatorCtor.newInstance(distributions, smallTextPool);
			} else if(generatorClass.equals(PartSupplierGenerator.class)) {
				generatorCtor = generatorClass.getConstructor(double.class, int.class, int.class);
				generator = generatorCtor.newInstance(scale, partNo, totalParts);
			} else {
				// use full constructor
				generatorCtor = generatorClass.getConstructor(double.class, int.class, int.class, Distributions.class, TextPool.class);
				generator = generatorCtor.newInstance(scale, partNo, totalParts, distributions, smallTextPool);
			}
			iter = generator.iterator();
		} catch (Throwable e) {
			throw new RuntimeException("Unable to create generator "+generatorClass, e);
		}
	}
	@Override
	public boolean hasNext() {
		return iter.hasNext();
	}

	@Override
	public T next() {
		return iter.next();
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("Remove not supported on this iterator");
	}
}
