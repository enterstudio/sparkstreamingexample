package sparkstreaming

import org.apache.spark._
import org.apache.spark.streaming._


object SparkStreamingExample {
  def main(args: Array[String]) =
    println("Hello, Spark!")

    // Create a configuration with 2 threads running on the local machine; name of the app is "HelloSparkStreaming"
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("HelloSparkStreaming")

    // Create a streaming context that batches data every 1 second
    val streamingContext = new StreamingContext(sparkConf, Seconds(15))

    // Create a file stream to a data directory
    val lines = streamingContext.textFileStream("data")

    // Process the lines in the file (split into words and then perform a map/reduce to count the words)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x,1)).reduceByKey(_ + _)
    wordCounts.print()

    // Start the streaming context and wait to finish
    streamingContext.start()
    streamingContext.awaitTermination()
}

