import java.sql.{Connection, DriverManager, ResultSet, ResultSetMetaData, Statement}

import scala.collection.mutable.ListBuffer


object Postgresaccess {

  def connect(host: String, dbname: String, user: String, password: String, port: String = "5432"): Connection = {

    //connect to db and return connection object
    val driver = "org.postgresql.Driver"
    val url = s"jdbc:postgresql://$host:$port/$dbname"
    Class.forName(driver)
    val connection = DriverManager.getConnection(url, user, password)
    return connection
  }

  def infer_schema(rsschema: ResultSetMetaData): List[String] = {

    //analyses metadata gets number of columns and what their names are
    val width = rsschema.getColumnCount
    //list of column names
    var buf_colnames = new ListBuffer[String]()
    for (i <- 1 to width) buf_colnames += (rsschema.getColumnName(i))
    val schema = buf_colnames.toList

    return schema
  }

  def fetch_result_stream(resultSet: ResultSet): Stream[ResultSet] = {
    new Iterator[ResultSet] {
      def hasNext = resultSet.next()
      def next() = resultSet
    }.toStream
  }

  def fetch_result_object(resultiterator: Stream[ResultSet], schema :List[String]):List[Map[String,Object]] = {
    // list buffer for the individual result maps
    var buf_results = new ListBuffer[Map[String,Object]]

    for (i <- resultiterator) {
      //create mutable placeholder for indiviudal row of data
      val buf_row = scala.collection.mutable.Map[String,Object]()

      //create list of tuples of column name, and the Object itself
      //Object type also containes a method to get the data type of the column which can be useful
      // for reinserting

      for ((k) <- schema) buf_row += (k -> i.getObject(k))
      buf_results += buf_row.toMap
    }

    //converts mutable buf_results object to immutable list object filled with dicationaries
    val result_object = buf_results.toList
    return result_object
  }

  def main(args: Array[String]): Unit = {

    var conn = connect("localhost", "scalabackend", "scala", "scalaload", "5432")
    val statement = conn.createStatement

    //Query a table from your postgres database
    val result = statement.executeQuery("Select * from load_scala.load_json")
    conn.close()

    //holds data in a lazy form until it is requested
    val resultiterator = fetch_result_stream(result)

    //analyse metadata get columns to fetch
    val schema = infer_schema(result.getMetaData)

    //get result object by fetching everything from the iterator stream
    val results = fetch_result_object(resultiterator,schema)

    println(results)
    println("result set is %s rows long".format(results.length))
    println("first row of results = " + results(1))
    //get selected column of first entry
    println("the price of the first set of results is " + results(1)("dwh_time"))
    println("the data type of the first set of results is "  + results(1)("dwh_time").getClass)
  }
}