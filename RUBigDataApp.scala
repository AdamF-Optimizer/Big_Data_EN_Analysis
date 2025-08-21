package org.rubigdata

// Class definitions we need in the remainder:
import org.apache.hadoop.io.NullWritable
import de.l3s.concatgz.io.warc.{WarcGzInputFormat, WarcWritable}
import de.l3s.concatgz.data.WarcRecord
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.jsoup.Jsoup
import java.net.URL

object RUBigDataApp {
  def main(args: Array[String]): Unit = {
    //val warcfile = "/cc-crawl/segments/1618038073437.35/warc/CC-MAIN-20210413152520-20210413182520-00135.warc.gz"
    val warcfiles = "/cc-crawl/segments/1618038073437.35/warc/CC-MAIN-20210413152520-20210413182520-0063*.warc.gz" //Last 10 warc files for this segment

    // Overrule default settings
    val sparkConf = new SparkConf()
      .setAppName("RUBigData WARC4Spark 2024")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[WarcRecord]))
    //                      .set("spark.dynamicAllocation.enabled", "true")
    implicit val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = sparkSession.sparkContext

    val warcs = sc.newAPIHadoopFile(
      warcfiles,
      classOf[WarcGzInputFormat],
      classOf[NullWritable],
      classOf[WarcWritable]
    )

    // Function to extract top-level domain from a given domain using regex
    def extractTopLevelDomain(domain: String) = {
      val topLevelDomainRegEx = """\.([^.]+)$""".r //RegEx to skip all characters until the last '.' and retrive what follows. Doesn't correctly parse "... .co.uk".
      topLevelDomainRegEx.findFirstMatchIn(domain).map(x => "." + x.group(1)) //prepend a . to the top level domain
    }


    val wb = warcs.map { wr =>
      val httpBody = wr._2.getRecord().getHttpStringBody()
      val removedHTML = Jsoup.parse(httpBody)
      val text = removedHTML.text()
      val wh = wr._2.getRecord().getHeader()
      val urlOption = Option(wh.getUrl())
      urlOption.flatMap { url =>
        if (url != null) {
            val domain = new URL(url).getHost // Useful java package method that can extract the Host name from a URL
            val topLevelDomain = extractTopLevelDomain(domain).getOrElse("")
            Some((text, topLevelDomain))
        } else {
          None
        }
      }
    }

    // Set of words with American spelling
    val americanWords: Set[String] = Set("color", "center")

    // Set of words with British spelling
    val britishWords: Set[String] = Set("colour", "centre")

    // Broadcast in order to have access to the sets in each executor and prevent "Task not Serializable" errors
    val americanWordsBroadcast = sc.broadcast(americanWords)
    val britishWordsBroadcast = sc.broadcast(britishWords)

    // Function to count occurrences of words from the set in a text and pair with the accompanying top level domain
    def countWordOccurrencesAndTopLevelDomain(textAndTopLevelDomain:(String, String), langaugeWords:Set[String])= {
      val (text, topLevelDomain) = textAndTopLevelDomain
      val words = text.toLowerCase.split("\\W+") //Transform to lowercase and remove all non-word letters (don't -> dont, feedback. -> feedback)
      val wordCount = words.count(langaugeWords.contains)
      if (wordCount >= 1)
        (topLevelDomain, wordCount)
      else
        ("", 0)
    }

    val americanResult = wb.map(x => countWordOccurrencesAndTopLevelDomain(x.getOrElse(("", "")), americanWordsBroadcast.value))
    println("Counts for American word occurrences per Top Level Domain:")
    val americanResultPerTopLevelDomain = americanResult.filter{case (a, b) => b != 0}.reduceByKey(_ + _).sortBy(-_._2)
    americanResultPerTopLevelDomain.take(100).foreach(println)
    println("Total American word occurrences:")
    println(americanResultPerTopLevelDomain.map{case (topLevelDomain, count) => count}.sum())

    val britishResult = wb.map(x => countWordOccurrencesAndTopLevelDomain(x.getOrElse(("", "")), britishWordsBroadcast.value))
    println("Counts for British word occurrences per Top Level Domain:")
    val britishResultPerTopLevelDomain = britishResult.filter{case (a, b) => b != 0}.reduceByKey(_ + _).sortBy(-_._2)
    britishResultPerTopLevelDomain.take(100).foreach(println)
    println("Total British word occurrences:")
    println(britishResultPerTopLevelDomain.map{case (topLevelDomain, count) => count}.sum())
  }
}
