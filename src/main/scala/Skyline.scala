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
  sc.stop()
}
