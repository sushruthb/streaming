package com.spark.ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.tuning.ParamGridBuilder
import com.spark.ml.MLUtils.accuracyScore
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.ml.feature.OneHotEncoderEstimator
import org.apache.spark.ml.tuning.CrossValidator

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
object TitanicPrediction {

  def main(args:Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)
      val spark=SparkSession.builder().appName("ML").getOrCreate()

   // val df =spark.read.csv("src/main/resources/ml/titanic.csv")
   val df =spark
     .read
       .option("header","true")
     .option("inferSchema","true")
     .csv("/user/hdfs/data/titanic.csv")
    df.printSchema()

  /*  val dropVals=df.drop("PassengerId","Name","SibSp","Parch","Ticket","Cabin","Embarked")
    dropVals.show(5)*/

    //handle missing values
    val meanValue = df.agg(mean(df("Age"))).first.getDouble(0)
    val fixedDf = df.na.fill(meanValue, Array("Age"))

    //test and train split
    val dfs = fixedDf.randomSplit(Array(0.7, 0.3))
    val trainDf = dfs(0).withColumnRenamed("Survived", "label")
    val crossDf = dfs(1)


    // create pipeline stages for handling categorical
    val genderStages = handleCategorical("Sex")
    val embarkedStages = handleCategorical("Embarked")
    val pClassStages = handleCategorical("Pclass")

    //columns for training
    val cols = Array("Sex_onehot", "Embarked_onehot", "Pclass_onehot", "SibSp", "Parch", "Age", "Fare")
    val vectorAssembler = new VectorAssembler().setInputCols(cols).setOutputCol("features")


    //algorithm stage
    val randomForestClassifier = new RandomForestClassifier()
    //pipeline
    val preProcessStages = genderStages ++ embarkedStages ++ pClassStages ++ Array(vectorAssembler)
    val pipeline = new Pipeline().setStages(preProcessStages ++ Array(randomForestClassifier))


    val model = pipeline.fit(trainDf)
    println("train accuracy with pipeline" + accuracyScore(model.transform(trainDf), "label", "prediction"))
    println("test accuracy with pipeline" + accuracyScore(model.transform(crossDf), "Survived", "prediction"))

    //cross validation
    val paramMap = new ParamGridBuilder()
      .addGrid(randomForestClassifier.impurity, Array("gini", "entropy"))
      .addGrid(randomForestClassifier.maxDepth, Array(1,2,5, 10, 15))
      .addGrid(randomForestClassifier.minInstancesPerNode, Array(1, 2, 4,5,10))
      .build()

    val cvModel = crossValidation(pipeline, paramMap, trainDf)
    println("train accuracy with cross validation" + accuracyScore(cvModel.transform(trainDf), "label", "prediction"))
    println("test accuracy with cross validation " + accuracyScore(cvModel.transform(crossDf), "Survived", "prediction"))


    val testDf = spark.read.option("header", "true").option("inferSchema", "true").csv("/user/hdfs/data/test.csv")
    val fareMeanValue = df.agg(mean(df("Fare"))).first.getDouble(0)
    val fixedOutputDf = testDf.na.fill(meanValue, Array("age")).na.fill(fareMeanValue, Array("Fare"))
    generateOutputFile(fixedOutputDf, cvModel)

    spark.stop()
  }

    def handleCategorical(column: String): Array[PipelineStage] = {
      val stringIndexer = new StringIndexer().setInputCol(column)
        .setOutputCol(s"${column}_index")
        .setHandleInvalid("skip")
      val oneHot = new OneHotEncoderEstimator().setInputCols(Array(s"${column}_index")).setOutputCols(Array(s"${column}_onehot"))
      Array(stringIndexer, oneHot)
    }



    def crossValidation(pipeline: Pipeline, paramMap: Array[ParamMap], df: DataFrame): Model[_] = {
      val cv = new CrossValidator()
        .setEstimator(pipeline)
        .setEvaluator(new BinaryClassificationEvaluator)
        .setEstimatorParamMaps(paramMap)
        .setNumFolds(5)
      cv.fit(df)
    }
  def generateOutputFile(testDF: DataFrame, model: Model[_]) = {
    val scoredDf = model.transform(testDF)
    val outputDf = scoredDf.select("PassengerId", "prediction")
    val castedDf = outputDf.select(outputDf("PassengerId"), outputDf("prediction").cast(IntegerType).as("Survived"))
    castedDf.write.format("csv").option("header", "true").mode(SaveMode.Overwrite).save("/user/hdfs/data/output/")
  }



}
