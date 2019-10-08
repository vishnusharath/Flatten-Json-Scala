import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, StructType}


object FlattenJson extends App {
  val conf = new SparkConf().setMaster("local[*]").setAppName("JSON Flattener")
  val sc = new SparkContext(conf)

  val sqlContext = new SQLContext(sc)

  val inputJson =
    """{"id":1,"name":"John","country":[{"code":"France","payment":[{"currency":"CNY"},{"currency":"EUR"}]},
      |{"code":"Indonesia","payment":[{"currency":"CNY"},{"currency":"EUR"},{"currency":"BYR"}]},
      |{"code":"China","payment":[{"currency":"EUR"},{"currency":"EUR"},{"currency":"NOK"}]}]}""".stripMargin

  println(inputJson)

  //creating rdd for the json
  val jsonRDD = sc.parallelize(inputJson :: Nil)
  //creating DF for the json
  val jsonDF = sqlContext.read.json(jsonRDD)

  //Schema of the JSON DataFrame before Flattening
  jsonDF.schema

  //Output DataFrame Before Flattening
  jsonDF.show(false)

  //Function for exploding Array and StructType column

  def flattenDataframe(df: DataFrame): DataFrame = {

    val fields = df.schema.fields
    val fieldNames = fields.map(x => x.name)
    val length = fields.length

    for (i <- 0 to fields.length - 1) {
      val field = fields(i)
      val fieldtype = field.dataType
      val fieldName = field.name
      fieldtype match {
        case arrayType: ArrayType =>
          val fieldNamesExcludingArray = fieldNames.filter(_ != fieldName)
          val fieldNamesAndExplode = fieldNamesExcludingArray ++ Array(s"explode_outer($fieldName) as $fieldName")
          // val fieldNamesToSelect = (fieldNamesExcludingArray ++ Array(s"$fieldName.*"))
          val explodedDf = df.selectExpr(fieldNamesAndExplode: _*)
          return flattenDataframe(explodedDf)
        case structType: StructType =>
          val childFieldnames = structType.fieldNames.map(childname => fieldName + "." + childname)
          val newfieldNames = fieldNames.filter(_ != fieldName) ++ childFieldnames
          val renamedcols = newfieldNames.map(x => (col(x.toString()).as(x.toString().replace(".", "_"))))
          val explodedf = df.select(renamedcols: _*)
          return flattenDataframe(explodedf)
        case _ =>
      }
    }
    df
  }

  val flattendedJSON = flattenDataframe(jsonDF)
  //schema of the JSON after Flattening
  flattendedJSON.schema

  //Output DataFrame After Flattening
  flattendedJSON.show(false)


}
