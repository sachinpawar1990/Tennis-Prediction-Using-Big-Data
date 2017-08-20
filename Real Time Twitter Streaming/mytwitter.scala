package demo.twitter

import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.flume._

/**
 * @author training
 */

object mytwitter {
  val conf = new SparkConf().setMaster("local[3]").setAppName("Tennis Tweets")
  val sc = new SparkContext(conf)

  def main(args: Array[String]) {

    sc.setLogLevel("ERROR")
    //sc.setLogLevel("WARN")

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val playerName = args.takeRight(args.length - 4)

    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val ssc = new StreamingContext(sc, Seconds(5))
    
    val playerTweets = TwitterUtils.createStream(ssc, None, playerName)
    
    val playerStatuses = playerTweets.map(status => status.getText())
    playerStatuses.print()  
        
    ssc.start()
    ssc.awaitTermination()
  }
}