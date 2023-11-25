import scala.io.Source

object Hello extends App {
  val filePath = "/home/ozzy/Downloads/normal.txt"
  val source = Source.fromFile(filePath)

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

  def isDominant(p: Point, q: Point): Boolean = {
    p.coordinates.zip(q.coordinates).forall { case (coordP, coordQ) => coordP <= coordQ } &&
      p.coordinates.exists(_ < q.coordinates.head)
  }

  def findSkylineSet(points: List[Point]): List[Point] = {
    points.filterNot(p => points.exists(q => isDominant(q, p)))
  }

  val skylineSet = findSkylineSet(points)

  println("Skyline Set:")
  skylineSet.foreach(println)
}
