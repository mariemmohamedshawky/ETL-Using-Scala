import java.sql.{Connection, DriverManager}
import java.time.LocalDateTime
import scala.io.Source
import scala.collection.mutable

object Main {
  def main(args: Array[String]): Unit = {
    //DatabaseConnection
    val url = "jdbc:mysql://localhost:3306/scala"
    val driver = "com.mysql.cj.jdbc.Driver"
    val username = "root"
    val password = "root"
    var connection: Connection = null
    try {
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)
      connection.setAutoCommit(false)
    } catch {
      case _: Exception =>
        println("Can't connect to database")
    }

      //FunctionsToApply
      val plus1000: Int => Int = i => i + 1000
      val pos: Int => Int = i => i.abs
      val anon: String => String = str => {
        val r = new scala.util.Random
        val sb = new StringBuilder
        for (_ <- 1 to str.length) {
          var char = r.nextPrintableChar()
          while (!char.isLetter) {
            char = r.nextPrintableChar()
          }
          sb.append(char)
        }
        sb.toString
      }
      val capitalize: String => String = str => str.capitalize


      //Intialization
      var all: Map[Int, List[Any]] = Map()
      val transform= new mutable.HashMap[Int,List[(String)]].withDefaultValue(Nil)
      var datatype:Map[Int,List[String]]=Map()
      val statement = connection.createStatement

      //Create Table Logs
      val log = statement.executeUpdate("create table if not exists logs(date timestamp,message varchar(150))")
      connection.createStatement
      //Function insert into logs
      val insertlogs = (masseage:String)=>{
        val insert = statement.executeUpdate(s"insert into logs values('${LocalDateTime.now()}','$masseage')")
        connection.createStatement
      }

      //ExtractFiles
      var count = 0
      for (i <- Range inclusive(1, 5)) {
        val file = Source.fromFile(s"D:\\ITI\\etlscala\\Customers$i.csv")
        val lines = file.getLines
        all = all ++ lines.map { x =>
          val fields = x.split(",")
          count = count + 1
          (count, fields(0).toString, fields(1).toString, fields(2).toString)
        }.map { z => z._1 -> List(z._2, z._3, z._4) }.toMap
      }

      insertlogs("Data is Extracted successfully")
      println(all)
      var count2 = 0
      var count3=0
      //Reading Meta
      val metafile = Source.fromFile("D:\\ITI\\etlscala\\metadata.txt")
      val metalines = metafile.getLines
      val meta = metalines.map { x =>
        var col = x.split(",")
        count2 = count2 + 1
        (count2, col(0).toString, col(1).toString, col(2).toString, col(3))
      }.map { x => x._1 -> List(x._2, x._3, x._4, x._5) }.toMap

      println("MetaData " + meta)
      val header = all(1)
      all = all.-(1)
      //println(header(0))
      //Using Transformation
      meta.map((m: ((Int, List[Any]))) => {
        var count4=0
        var f: Int = m._1.toString.toInt
        if (header(f - 1) == meta(m._1)(0)) {

          println("===============================================================================")
          meta(m._1)(1) match {
            case "int" =>  datatype=datatype+(count3->List(meta(m._1)(0),"int"))
            case "string" => datatype=datatype+(count3->List(meta(m._1)(0),"varchar(50)"))
          }
          count3 = count3 + 1

          meta(m._1)(2) match {
            case "pos/plus1000" => var b = all.map((f:((Int,List[Any])))=>{
              if(all(f._1)(0).toString.toInt> 0) {
                var s:Int= plus1000(all(f._1)(0).toString.toInt)
                transform+=count4->((s.toString):: transform(count4))
                count4=count4+1
              }else {
                println("++++++++++++++++++++++++=")
                var d : Int = plus1000(pos(all(f._1)(0).toString().toInt))
                transform+=count4->((d.toString):: transform(count4))
                count4=count4+1
              }
            }
            )
            case "anon/capitalize" =>  all.map((f:((Int,List[Any])))=>{
              val name=anon(capitalize(all(f._1)(1).toString))
              transform+=count4->((name):: transform(count4))
              count4=count4+1
            })
            case "pos" => all.map((f:((Int,List[Any])))=>{
              if(all(f._1)(2).toString.toInt> 0) {
                val agee= all(f._1)(2).toString.toInt.toString
                transform+=count4->((agee):: transform(count4))
                count4=count4+1
                all(f._1)(2)
              }
            }
            )

          }
        }
      })

      println("Transformation"+transform)
      insertlogs("Data is Transformed successfully")

      //Create Tables
      meta.map((m: ((Int, List[Any]))) => {
        meta(m._1)(3) match {
          case "append" => val rs = statement.executeUpdate(s"create table if not exists customer(${datatype(0)(0)} ${datatype(0)(1)},${datatype(1)(0)} ${datatype(1)(1)},${datatype(2)(0)} ${datatype(2)(1)})")
          case "write" =>val wr = statement.executeUpdate("drop table if exists customer")
            val wrt=statement.executeUpdate(s"create table if not exists customer (${datatype(0)(0)} ${datatype(0)(1)},${datatype(1)(0)} ${datatype(1)(1)},${datatype(2)(0)} ${datatype(2)(1)})")
        }
        connection.createStatement
      })
      insertlogs("Tables is Created successfully")


    println(LocalDateTime.now())
      //Load Transformation Data into Database
      transform.map((l:(Int,List[Any]))=>{
        val sql=s"insert into customer (id,name,age) values(${transform(l._1)(2)},'${transform(l._1)(1)}',${transform(l._1)(0)})"



        statement.addBatch(sql)

        //connection.createStatement

      })
    statement.executeBatch()


    println(LocalDateTime.now())
      insertlogs("Data is Loaded successfully")
    connection.commit()
    connection.close()
    }}

