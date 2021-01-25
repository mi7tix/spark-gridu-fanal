
package project2

import org.apache.spark.sql.{Dataset, Row, SaveMode, Column, DataFrame}
import org.apache.spark.sql.types._

object Project2 extends App{

  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions._

val userDataPath="src/main/resources/user_dir.avro"
val userMsgDataPath="src/main/resources/msg_dir.avro"
val msgDataPath="src/main/resources/msg.avro"
val retweetDataPath="src/main/resources/retweet.avro"

val userDF = spark.read.format("avro").load(userDataPath)
val msgDirDF = spark.read.format("avro").load(userMsgDataPath)
val msgDF = spark.read.format("avro").load(msgDataPath)
val retweetDF = spark.read.format("avro").load(retweetDataPath)



val wave1 = msgDF.as("msg")
    .join(retweetDF.as("rtw"), $"msg.USER_ID" === $"rtw.USER_ID")
    .select(
        $"rtw.USER_ID",
        $"rtw.SUBSCRIBER_ID",
        $"rtw.MESSAGE_ID")
    .groupBy($"USER_ID", $"MESSAGE_ID")
    .agg(count($"SUBSCRIBER_ID").as("RETWEETS_COUNT"))
    .orderBy($"RETWEETS_COUNT")


    .filter($"w1.USER_ID" === $"rtw.USER_ID")

val wave2 = wave1.as("w1")
    .join(retweetDF.as("rtw"), $"w1.USER_ID" === $"rtw.USER_ID")
    .select(
        $"rtw.USER_ID",
        $"rtw.SUBSCRIBER_ID",
        $"rtw.MESSAGE_ID")
    .groupBy($"USER_ID", $"MESSAGE_ID")
    .agg(count($"SUBSCRIBER_ID").as("RETWEETS_COUNT")).limit(10)
    .orderBy($"RETWEETS_COUNT")



val wave1Cnt = wave1.as("topr")
      .join(
      userDF.as("user"),$"topr.USER_ID" === $"user.USER_ID")
      .join(msgDirDF.as("msgd"),$"topr.MESSAGE_ID" === $"msgd.MESSAGE_ID")
      .select(
      $"topr.USER_ID",
      $"user.FIRST_NAME",
      $"user.LAST_NAME",
      $"topr.MESSAGE_ID",
      $"msgd.TEXT",
      $"topr.RETWEETS_COUNT".as("NUMBER_RETWEETS")
    )



val wave2Cnt = wave2.as("topr")
      .join(
      userDF.as("user"),$"topr.USER_ID" === $"user.USER_ID")
      .join(msgDirDF.as("msgd"),$"topr.MESSAGE_ID" === $"msgd.MESSAGE_ID")
      .select(
      $"topr.USER_ID",
      $"user.FIRST_NAME",
      $"user.LAST_NAME",
      $"topr.MESSAGE_ID",
      $"msgd.TEXT",
      $"topr.RETWEETS_COUNT".as("NUMBER_RETWEETS")
    )


val fin_ds = wave1Cnt.union(wave2Cnt)

}


