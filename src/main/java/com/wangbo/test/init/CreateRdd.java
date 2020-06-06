package com.wangbo.test.init;

import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.Dependency;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partition;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.junit.Test;

import scala.Tuple2;
import scala.collection.Seq;

/**
 * Spark 2.4.4支持 lambda表达式 以简洁地编写函数,在Spark 2.2.0中已删除了对Java 7的支持
 * @author 0380008788
 */
public class CreateRdd implements Serializable{
	private static final long serialVersionUID = -4235837540380692315L;

	/**
	 * 	在Spark中闭包的重要性: 访问了声明在函数外部的一个或多个变量的函数即定义为闭包。
	 * 		1.在java中，匿名内部类如果访问了作用域外的变量，即为闭包，而且该变量默认被标记为final；
	 * 		2.spark中定义闭包的过程就是捕获外部变量（获取值或对象存储地址）而构成一个封闭的函数；
	 * 		3.Spark跨集群执行RDD操作时，执行任务前会将计算的闭包序列化并发送给每个执行器，因此在执行程序中
	 * 		计算函数引用的变量不再是驱动程序中的变量，而是其副本；对副本的修改不会影响原变量的值和内容
	 */
	@Test
	public void testEnclosingScope() {
		SparkConf conf = new SparkConf().setAppName("sparkTest").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		People lily = new People();
		JavaRDD<Integer> listRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6), 4);
		listRDD.foreach(i -> {
			// 在各个执行程序中创建各自的People对象-->lily,均为深复制副本
			System.out.println(lily);
			lily.setName(i + lily.getName());	
		});

		System.out.println(lily.getName()); //lily
		sc.close();
		
		Stream<Integer> listStream = Stream.of(1,2,3,4,5);
		listStream.forEach(i -> lily.setName(i + lily.getName()));
		System.out.println(lily.getName()); //54321lily
	}
	
	/**
	 * 在Spark集群模式中，由于任务被分派到不同的执行程序中执行，因此函数中的打印执行在驱动程序的控制台中不会打印显示
	 */
	@Test
	public void testDifferentProgress() {
		SparkConf conf = new SparkConf().setAppName("sparkTest").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<Integer> listRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6), 4);
		listRDD.foreach(i -> {
			String name = ManagementFactory.getRuntimeMXBean().getName();
			System.out.println(name + ";输出值==" + i);
			System.out.println(Thread.currentThread().getName() + ";输出值==" + i);
		});

		sc.close();
	}
	
	/**
	 * 	由于Spark执行任务前会将计算的闭包序列化并发送给每个执行器
	 * 		1.在RDD操作中，spark算子本身（接口lambda表达式）必须实现序列化接口Serializable；
	 * 		2.如果算子引用了某类的成员变量或成员函数，会导致该类及所有成员都需要支持序列化；
	 * 		3.如果想要某些成员变量不序列化，则加上关键字transient标注避免序列化影响；
	 *  特别情况：
	 *  	如果类的成员变量没有赋值，则该成员变量不会序列化；类的静态变量也不会序列化; 引用类的静态方法不需要该类实现序列化
	 */
	@Test
	public void testSerializable() {
		SparkConf conf = new SparkConf().setAppName("sparkTest").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		People lily = new People();
		// people中的静态变量obj不需要序列化
		People.obj = new Son();
		lily.setName("lily");
		// 由于people中的son成员变量被transient标注，因此类Son不需要实现序列化
		Son son = new Son();
		lily.setSon(son);
		
		JavaRDD<Integer> listRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6), 4);
		listRDD.foreach(i -> {
			// 引用MyUtills的静态方法不需要MyUtills实现序列化
			System.out.println(MyUtills.getName());
			lily.setName(i + lily.getName());	
		});

		System.out.println(lily.getName()); //lily
		sc.close();
	}
	
	/**
	 * 	大多数Spark操作可以作用于任何类型的RDD，但有部分“分布式洗牌”操作只能作用于键值对RDD（PairRDD）
	 * 		1.可以通过mapToPair和flatMapToPair等将RDD转成PairRDD；
	 * 		2.PairRDD中的单个元素是scala.Tuple2，可以通过new Tuple2(a, b)创建；
	 * 		3.PairRDD可以使用特殊的分组、聚合等spark操作；
	 */
	@Test
	public void testPairRDD() {
		SparkConf conf = new SparkConf().setAppName("pairRDD").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
//		JavaRDD<String> fileRDD = sc.textFile("F:\\ceshi.txt");
		JavaRDD<String> fileRDD = sc.parallelize(Arrays.asList("abc","bd","c","abc","d","e","bd","c","abc"));
		System.out.println("默认分区数：" + fileRDD.getNumPartitions()); // 1
		
		JavaPairRDD<String, Integer> pairRDD = fileRDD.mapToPair(s -> new Tuple2<String,Integer>(s, 1));
		JavaPairRDD<String, Integer> reduceRDD = pairRDD.reduceByKey((a, b) -> a + b, 4).cache();
		reduceRDD.foreach(t -> {
			System.out.println(t._1 + "出现的次数为：" + t._2);
		});
		/**
		 * MyComparator接口是类CreateRDD的成员对象，引用接口需要CreateRDD也实现序列化
		 */
		List<Tuple2<String, Integer>> resultList = reduceRDD
				.sortByKey(new MyComparator<String>() {
					private static final long serialVersionUID = -7785851970495713968L;

					@Override
					public int compare(String o1, String o2) {
						return o1.length() - o2.length();
					}
				}, true).collect();
//		        .sortByKey((key1, key2) -> key1.length() - key2.length(), true).collect();
		sc.close();
		resultList.forEach(t -> System.out.println(t._1 + "出现的次数为：" + t._2));
	}
	
	interface MyComparator<T> extends Comparator<T>, Serializable {
	}
	
	@Test
	public void testGroup() {
		SparkConf conf = new SparkConf().setAppName("Group").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<People> peopleRDD = sc.parallelize(Arrays.asList(new People("lily", 12),
		        new People("han", 13), new People("meimei", 14), new People("dudu", 18)));
		JavaPairRDD<Integer, Iterable<People>> groupRDD1 = peopleRDD.groupBy(People::getAge, 4);
		
		JavaRDD<String> fileRDD = sc.parallelize(Arrays.asList("abc","bd","c","abc","d","e","bd","c","abc"),5);
		JavaPairRDD<String, Integer> stringRDD = fileRDD.mapToPair(s -> new Tuple2<String,Integer>(s, 1));
		JavaPairRDD<String, Integer> acountRDD = stringRDD.reduceByKey((a,b) -> a+b);
		acountRDD.foreach(t -> {
			System.out.println(t._1 + "出现的次数为：" + t._2);
		});
		sc.close();
	}
	
	@SuppressWarnings("serial")
	@Test
	public void testAggregate() {
		SparkConf conf = new SparkConf().setAppName("Group").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		/**
		 * 	设置spark的日志等级：
		 * 		1.在程序中设置spark的日志打印级别：sc.setLogLevel()或者Logger.getLogger("org.apache.spark").setLevel()
		 *	 	2.在主配置文件中配置org.apache.spark的级别，spark默认采用spark-core包中log4j-defaults.properties作为日志配置
		 */
//		sc.setLogLevel(Level.ERROR.toString());
//		Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
		
		List<Tuple2<String, Integer>> abk = Arrays.asList(new Tuple2<String, Integer>("class1", 1),
		        new Tuple2<String, Integer>("class1", 2), new Tuple2<String, Integer>("class1", 4),
		        new Tuple2<String, Integer>("class2", 3), new Tuple2<String, Integer>("class2", 1),
		        new Tuple2<String, Integer>("class2", 5), new Tuple2<String, Integer>("class3", 2),
		        new Tuple2<String, Integer>("class3", 3), new Tuple2<String, Integer>("class3", 4));
		JavaPairRDD<String, Integer> abkrdd = sc.parallelizePairs(abk, 5).cache();
		/**
		 * 	根据方法mapPartitionsWithIndex算子中的打印可以知道数据集的分区情况
		 */
		abkrdd.mapPartitionsWithIndex(
		        new Function2<Integer, Iterator<Tuple2<String, Integer>>, Iterator<String>>() {
			        @Override
			        public Iterator<String> call(Integer s, Iterator<Tuple2<String, Integer>> v)
			                throws Exception {
				        List<String> li = new ArrayList<>();
				        while (v.hasNext()) {
					        li.add("data：" + v.next() + " in " + (s + 1) + " " + " partition");
				        }
				        return li.iterator();
			        }
		        }, true).foreach(m -> System.out.println(m));
		/**
		 *  聚合方法aggregateByKey的四个参数：
		 *  	1.zeroValue表示每个分区的键对应的初始对比值，只在算子seqFunc中使用，
		 *  	例如abkrdd的分区4中有（'class2',5）、('class3',2),则会对应增加初始值*（'class2',zeroValue）、（'class3',zeroValue）
		 *  	2.算子seqFunc的运算在每个分区分别执行reduceByKey运算 => 即按键分解
		 *  	3.partitioner或numPartitions表示执行算子combFunc时的可用分区,如果未指定使用abkrdd的分区数
		 *  	（将计算放入numPartitions个分区中计算）
		 *  	4.算子combFunc将执行完算子seqFunc后数据集一起执行reduceByKey运算 => 即按键分解
		 */
		JavaPairRDD<String, Integer> abkrdd2 = abkrdd.aggregateByKey(0, 6,
		        new Function2<Integer, Integer, Integer>() {
			        @Override
			        public Integer call(Integer s, Integer v) throws Exception {
				        System.out.println("seq:" + s + "," + v);
				        return Math.max(s, v);
			        }
		        }, new Function2<Integer, Integer, Integer>() {
			        @Override
			        public Integer call(Integer s, Integer v) throws Exception {
				        System.out.println("com:" + s + "," + v);
				        return s + v;
			        }
		        });

		abkrdd2.foreach(new VoidFunction<Tuple2<String, Integer>>() {
			@Override
			public void call(Tuple2<String, Integer> s) throws Exception {
				System.out.println("c:" + s._1 + ",v:" + s._2);
			}
		});

		sc.close();
	}
	
	/**
	 * 1. 每个Spark应用程序都包含一个驱动程序，该程序运行用户的main功能并在集群上执行各种并行操作
	 * 
	 * 2. Spark提供的主要抽象是弹性分布式数据集（RDD）
	 * 
	 * 3. 第二个抽象是可以在并行操作中使用的共享变量
	 * Spark支持两种类型的共享变量：广播变量（可用于在所有节点上的内存中缓存值）和累加器（accumulator）
	 */
	@Test
	public void createContext() {
		// appName参数是应用程序显示在集群UI上的名称, master是Spark，Mesos或YARN群集URL或特殊的“本地”字符串
		SparkConf conf = new SparkConf().setAppName("sparkTest").setMaster("local");
		// 创建一个JavaSparkContext对象，该对象告诉Spark如何访问集群
		JavaSparkContext sc = new JavaSparkContext(conf);
		/**
		 * RDD是可并行操作的元素的容错集合
		 * 创建RDD的方法有两种:
		 * 		1.并行化 驱动程序中的现有集合
		 * 		2.引用外部存储系统（例如共享文件系统，HDFS，，Cassandra，HBase，Amazon S3等或提供Hadoop InputFormat的任何数据源）中的数据集
		 */
		// 并行集合的一个重要参数是将数据集切入的分区数numSlices -> sc.defaultParallelism
		JavaRDD<Integer> listRDD = sc.parallelize(Arrays.asList(1,2,3,4,5));
		listRDD.foreach(cha -> System.out.println(cha));
		
		/**
		 *  方法需要一个URI的文件（本地路径的机器上，或一个hdfs://，s3a://等URI）
		 *  Spark的所有基于文件的输入方法（包括textFile）都支持在目录，压缩文件和通配符上运行。例如，你可以使用
		 *  textFile("/my/directory")，textFile("/my/directory/*.txt")和textFile("/my/directory/*.gz")
		 */
		JavaRDD<String> fileRDD = sc.textFile("F:\\文档\\阅读数据.txt")
				// 将RDD 保留在内存中,避免每次计算转换后的RDD
				.cache()
				// 将RDD持久存储在磁盘上，或在多个节点上复制
//				.persist(StorageLevels.MEMORY_AND_DISK_2)
				;
		// 将所有行的大小相加
		Integer totalSize = fileRDD.map(line -> line.length()).reduce((a, b) -> a + b);
		System.out.println(totalSize);
		
		JavaRDD<String> flatMap = fileRDD
		        .flatMap(line -> Arrays.asList(line.split(",")).iterator());
		
		// 读取包含多个小文本文件的目录，并将每个小文本文件作为（文件名，内容）对返回
//		JavaPairRDD<String, String> wholeFileRDD = sc.wholeTextFiles("F:\\文档");
		
		// key和value应该是Hadoop的Writable接口的子类，例如IntWritable和Text；sequenceFile[Int, String]将自动读取IntWritables和Texts
//		JavaPairRDD<IntWritable, Text> sequenceRDD = sc.sequenceFile("F:\\文档\\阅读数据.txt", IntWritable.class, Text.class);
		
		// 以包含序列化Java对象的简单格式保存RDD
//		JavaRDD<Object> objectRDD = sc.objectFile("F:\\文档\\阅读数据.txt");
		/**
		 * Spark 2.0之后，RDD被Database取代
		 */
		
		sc.close();
	}
	
	public void createSession() {
		// appName参数是应用程序显示在集群UI上的名称, master是Spark，Mesos或YARN群集URL或特殊的“本地”字符串
		SparkConf conf = new SparkConf().setAppName("configTest").setMaster("local");
		SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
	
		sparkSession.close();
	}
	
	@Test
	public void testAccumulator() {
		SparkConf conf = new SparkConf().setAppName("Accumulator").setMaster("local[3]");
		JavaSparkContext sc = new JavaSparkContext(conf);
//		sc.setCheckpointDir(dir);
		
		LongAccumulator accumulator = sc.sc().longAccumulator();
		JavaRDD<Integer> listRDD = sc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9),4);
		listRDD.foreach(i -> accumulator.add(i));
		System.out.println("数组累计之和为： " + accumulator.value());
		
		sc.stop();
	}
	
	
}
