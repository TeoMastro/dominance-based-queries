import scala.io.Source
import org.apache.spark.{SparkConf, SparkContext}

object Skyline extends App {
  val filePath = "/home/ozzy/Downloads/normal.txt"
  val source = Source.fromFile(filePath)
  val conf = new SparkConf().setAppName("SkylineApp").setMaster("local")
  val sc = new SparkContext(conf)

  case class Point(coordinates: List[Float])

  var points: List[Point] = List.empty
  try {
    val lines = source.getLines()
    // Process each line and convert it to a list of Point objects
    points = lines.map { line =>
      // Split the line into individual coordinates (assuming they are space-separated)
      val coordinates = line.split(",").map(_.toFloat).toList

      // Create a Point object from the coordinates
      Point(coordinates)
    }.toList
  } finally {
    // Make sure to close the source to release resources
    source.close()
  }

  val pointsRDD = sc.parallelize(points)

  // Task 1
  def isDominant(p: Point, q: Point): Boolean = {
    p.coordinates.zip(q.coordinates).forall { case (coordP, coordQ) => coordP <= coordQ } &&
      p.coordinates.exists(_ < q.coordinates.head)
  }

  def findSkylineSet(pointsRDD: org.apache.spark.rdd.RDD[Point]): org.apache.spark.rdd.RDD[Point] = {
    val collectedPoints = pointsRDD.collect()

    val dominanceChecks = pointsRDD.map(p =>
      (p, collectedPoints.filter(q => isDominant(q, p)).isEmpty)
    )
    dominanceChecks.filter { case (_, isNotDominated) => isNotDominated }.keys
  }

  val skylineSetRDD = findSkylineSet(pointsRDD)

  println("Skyline Set:")
  skylineSetRDD.foreach(println)

  // Task 2
  def calculateDominanceScore(pointsRDD: org.apache.spark.rdd.RDD[Point]): org.apache.spark.rdd.RDD[(Point, Int)] = {
    // Create a Cartesian product of the RDD with itself, @NOTE: THE WORST WAY TO DO THIS!!!
    val cartesianRDD = pointsRDD.cartesian(pointsRDD)

    // Filter the pairs where the first point dominates the second
    val dominatedPairs = cartesianRDD.filter { case (p1, p2) => p1 != p2 && isDominant(p1, p2) }

    // Count the number of times each point dominates others
    val dominanceCount = dominatedPairs.map { case (p1, _) => (p1, 1) }.reduceByKey(_ + _)

    dominanceCount
  }

  def findTopKDominant(pointsRDD: org.apache.spark.rdd.RDD[Point], k: Int): Array[(Point, Int)] = {
    calculateDominanceScore(pointsRDD).top(k)(Ordering.by(_._2))
  }

  val k = 5 // Number of points to return
  val topKDominantPoints = findTopKDominant(pointsRDD, k)
  println("The " + k + " points with the highest dominance score.")
  topKDominantPoints.foreach(println)

  // Task 3
  def calculatePairwiseDominance(pointsRDD: org.apache.spark.rdd.RDD[Point]): org.apache.spark.rdd.RDD[(Point, Point)] = {
    pointsRDD.cartesian(pointsRDD)
      .filter { case (p1, p2) => p1 != p2 && isDominant(p1, p2) }
  }

  def calculateDominanceScoresForSkyline(skylineRDD: org.apache.spark.rdd.RDD[Point], pairwiseDominanceRDD: org.apache.spark.rdd.RDD[(Point, Point)]): org.apache.spark.rdd.RDD[(Point, Int)] = {
    // Convert skylineRDD to a pair RDD with dummy values for joining
    val skylinePairRDD = skylineRDD.map(point => (point, None))

    pairwiseDominanceRDD
      .map { case (dominant, _) => (dominant, 1) }
      .join(skylinePairRDD) // Join with the skyline set @NOTE: THIS IS BAD FOR SCALING
      .map { case (dominant, (count, _)) => (dominant, count) }
      .reduceByKey(_ + _)
  }

  val pairwiseDominanceRDD = calculatePairwiseDominance(pointsRDD)

  def findTopKDominantInSkyline(skylineRDD: org.apache.spark.rdd.RDD[Point], allPointsRDD: org.apache.spark.rdd.RDD[Point], k: Int): Array[(Point, Int)] = {
    calculateDominanceScoresForSkyline(skylineSetRDD, pairwiseDominanceRDD).top(k)(Ordering.by(_._2))
  }

  val n = 2 // Number of points to return from the skyline
  val topKDominantInSkyline = findTopKDominantInSkyline(skylineSetRDD, pointsRDD, n)
  topKDominantInSkyline.foreach(println)
  sc.stop()
}
