package project2

import org.apache.spark.sql.SparkSession

object Data_Preparation extends App {

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("CreateDatasets")
      .master("local")
      .getOrCreate()

val userDir= Seq(
    (1,"Robert","Smith"),
    (2,"John","Johnson"),
    (3,"Alex","Jones"),
    (4,"Ryan","Chapman"),
    (5,"Leonard","Price"),
    (6,"Austin","Bailey"),
    (7,"Otto","Lee"),
    (8,"Ayden","Rogers"),
    (9,"Thomas","Matthews"),
    (10,"Simon","Burke"),
    (11,"Jaxson","Griffiths"),
    (12,"Ronnie","Edwards"),
    (13,"Ayden","Williamson")
    ).toDF("USER_ID","FIRST_NAME","LAST_NAME")

  

  val msgDir = Seq(
    (11,"text"),
    (12,"text"),
    (13,"text"),
    (14,"text"),
    (15,"text"),
    (16,"text"),
    (17,"text"),
    (18,"text"),
    (19,"text")
  ).toDF("MESSAGE_ID","TEXT")



  val message = Seq(
    (1,11),
    (2,12),
    (3,13),
    (4,14),
    (5,15),
    (6,16),
    (7,17),
    (8,18),
    (9,19)
  ).toDF("USER_ID","MESSAGE_ID")


  val retweet = Seq(
    (1,2,11),
    (1,3,11),
    (2,5,11),
    (2,6,11),
    (2,7,11),
    (3,7,11),
    (7,14,11),
    (5,33,11),
    (2,4,12),
    (3,8,13)
  ).toDF("USER_ID","SUBSCRIBER_ID","MESSAGE_ID")

  import spark.sqlContext.implicits._


  userDir.write.format("avro").save("src/main/resources/user_dir.avro")
  msgDir.write.format("avro").save("src/main/resources/msg_dir.avro")
  message.write.format("avro").save("src/main/resources/msg.avro")
  retweet.write.format("avro").save("src/main/resources/retweet.avro")

}
