package project2

import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.apache.spark.sql
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.io.{File, IOException}
import java.nio.ByteBuffer
import java.util

import com.google.common.io.Files

class UtilitiesTestSpec extends FunSuite with BeforeAndAfterEach {

 private val master = "local"

 private val appName = "TaskTest"

 var spark : SparkSession = _

 override def beforeEach(): Unit = {
   spark = new sql.SparkSession.Builder().appName(appName).master(master).getOrCreate()
  }

test("Reading an invalid file location using readTextfileToDataSet should throw an exception") {
        
      intercept[Exception] {
      val sparkSession = spark
      import org.apache.spark.sql.functions.col
      val df = ReadAndWrite.readFile(sparkSession,"")
      
      df.show()

      }
 }

  /**
   * This function checks that all records in a file match the original
   * record.
   */
  val avroDir = "src/main/resources/user_dir.avro"
  val testFile = "spart-00000-6b686ede-03f5-4197-82f2-3fe622e512d6-c000.avro"
  def checkReloadMatchesSaved(spark: SparkSession, testFile: String, avroDir: String) = {

    def convertToString(elem: Any): String = {
      elem match {
        case null => "NULL" // HashSets can't have null in them, so we use a string instead
        case arrayBuf: ArrayBuffer[_] =>
          arrayBuf.asInstanceOf[ArrayBuffer[Any]].toArray.deep.mkString(" ")
        case arrayByte: Array[Byte] => arrayByte.deep.mkString(" ")
        case other => other.toString
      }
    }

    val originalEntries = spark.read.avro(testFile).collect()
    val newEntries = spark.read.avro(avroDir).collect()

    assert(originalEntries.length == newEntries.length)

    val origEntrySet = Array.fill(originalEntries(0).size)(new HashSet[Any]())
    for (origEntry <- originalEntries) {
      var idx = 0
      for (origElement <- origEntry.toSeq) {
        origEntrySet(idx) += convertToString(origElement)
        idx += 1
      }
    }

    for (newEntry <- newEntries) {
      var idx = 0
      for (newElement <- newEntry.toSeq) {
        assert(origEntrySet(idx).contains(convertToString(newElement)))
        idx += 1
      }
    }
  }

  def withTempDir(f: File => Unit): Unit = {
    val dir = Files.createTempDir()
    dir.delete()
    try f(dir) finally deleteRecursively(dir)
  }