package icbc.put

import java.text.SimpleDateFormat
import java.util.Date
import java.util.logging._

import org.apache.spark.sql.{Column, DataFrame, RelationalGroupedDataset, SparkSession}


trait GetCurrentTime{
  def getCurrentTime:String = {
    val date = new Date()
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    dateFormat.format(date)
  }
}

trait Logging {
  val log = Logger.getLogger(".")
  log.setUseParentHandlers(false)
  log.addHandler(new ConsoleHandler)
  log.setLevel(Level.ALL)
  log.getHandlers.foreach(_.setLevel(Level.ALL))
  def error(msg:String)=log.severe(msg)
  def warn(msg:String)=log.warning(msg)
  def info(msg:String)=log.info(msg)
  def debug(msg:String)=log.fine(msg)
  def trace(msg:String)=log.finer(msg)
}

class MyLogHander extends Formatter with GetCurrentTime{
  def format(record:LogRecord):String={
    return getCurrentTime+" "+record.getLevel+":"+record.getMessage+"\n"
  }
}

class PublicUtilTools extends Logging {
  val sparkSession = SparkSession.builder.master("local[*]").appName("apworkload").getOrCreate()
//  val sparkSession = SparkSession.builder.master("spark://122.19.29.71:7077").appName("apworkload").getOrCreate()

  var df: DataFrame = _

  def load(tablename:String):DataFrame={
    if(tablename.equals("jira")||tablename.equals("questions")||tablename.equals("projInfo")||tablename.equals("projAppFPInfo")){
      if(tablename.equals("jira")){
        df = sparkSession.read.option("header","true").csv("hdfs://hybigdata001:8020/user/hadoop/hzyfzc/midResult/jira/"+tablename+"*")
      }
      if(tablename.equals("questions")){
        df = sparkSession.read.option("header","true").csv("hdfs://hybigdata001:8020/user/hadoop/hzyfzc/midResult/questions/"+tablename+"*")
      }
      if(tablename.equals("projInfo")){
        df = sparkSession.read.option("header","true").csv("hdfs://hybigdata001:8020/user/hadoop/hzyfzc/midResult/adlm/"+tablename+"*")
      }
      if(tablename.equals("projAppFPInfo")){
        df = sparkSession.read.option("header","true").csv("hdfs://hybigdata001:8020/user/hadoop/hzyfzc/midResult/adlm/"+tablename+"*")
      }
    }else{
//      df=sparkSession.read.option("header","true").csv(tablename)
      df = sparkSession.read.format("csv")
        .option("header", "true")
        .option("encoding", "gbk")
        .load(tablename)
    }
    df
  }

  def load2(tablename:String):DataFrame={
    if(tablename.equals("jira")||tablename.equals("questions")||tablename.equals("projInfo")||tablename.equals("projAppFPInfo")){
      if(tablename.equals("jira")){
//        spark.read.option("header", true).csv("/path/file.csv")
        df = sparkSession.read.option("header","true").csv("hdfs://hybigdata001:8020/user/hadoop/hzyfzc/midResult/jira/"+tablename+"*")
      }
      if(tablename.equals("questions")){
        df = sparkSession.read.option("header","true").csv("hdfs://hybigdata001:8020/user/hadoop/hzyfzc/midResult/questions/"+tablename+"*")
      }
      if(tablename.equals("projInfo")){
        df = sparkSession.read.option("header","true").csv("hdfs://hybigdata001:8020/user/hadoop/hzyfzc/midResult/adlm/"+tablename+"*")
      }
      if(tablename.equals("projAppFPInfo")){
        df = sparkSession.read.option("header","true").csv("hdfs://hybigdata001:8020/user/hadoop/hzyfzc/midResult/adlm/"+tablename+"*")
      }
      df
    }else{
      sparkSession.read.option("header","true").csv(tablename)
    }
  }

  def filter(df:DataFrame,conditionExpr:String):DataFrame={
    df.filter(conditionExpr)
  }

  def join(leftDF:DataFrame,rightDF:DataFrame,usingColumns:Seq[String],joinType:String):DataFrame={
    leftDF.join(rightDF,usingColumns,joinType)
  }

  def join(leftDF:DataFrame,rightDF:DataFrame,joinExprs:Column,joinType:String):DataFrame={
    leftDF.join(rightDF,joinExprs,joinType)
  }

  def groupBy(df:DataFrame,col1:String):RelationalGroupedDataset={
    df.groupBy(col1)
  }

  def groupBy(df:DataFrame,col1:String,col2:String):RelationalGroupedDataset={
    df.groupBy(col1,col2)
  }

  def groupBy(df:DataFrame,col1:String,col2:String,col3:String):RelationalGroupedDataset={
    df.groupBy(col1,col2,col3)
  }

  def groupBy(df:DataFrame,col1:String,col2:String,col3:String,col4:String):RelationalGroupedDataset={
    df.groupBy(col1,col2,col3,col4)
  }

  def count(ds:RelationalGroupedDataset):DataFrame={
    ds.count().repartition(1)
  }

  def count(df:DataFrame):Long={
    df.count()
  }

  def sort(df:DataFrame,col1:Column,col2:Column):DataFrame = {
    df.sort(col1,col2).repartition(1)
  }

  def sort(df:DataFrame,col1:Column,col2:Column,col3:Column):DataFrame = {
    df.sort(col1,col2,col3).repartition(1)
  }

  def sort(df:DataFrame,col1:Column,col2:Column,col3:Column,col4:Column):DataFrame = {
    df.sort(col1,col2,col3,col4).repartition(1)
  }

  def sort(df:DataFrame,col1:Column):DataFrame = {
    df.sort(col1).repartition(1)
  }

  def sortInternal(df:DataFrame,version:String,dutyDept:String):DataFrame={
    val sortDept = sparkSession.read.option("header","true").csv("xxxxx")
    val joinedDF = df.join(sortDept,"dutyDept")
    val sortedDF = joinedDF.sort(joinedDF("version").desc,joinedDF("deptNo")).drop("deptNo").repartition(1)
    sortedDF
  }

  def getSaveTime:String={
    val date=new Date()
    val dateFormat = new SimpleDateFormat("yyyyMMddHHmm")
    dateFormat.format(date)
  }

  def filterInternale(df:DataFrame,dept:String):DataFrame={
      val hzDept="'杭州开发一部','杭州开发二部','杭州开发三部','杭州开发四部','杭州技术部','云计算实验室',"
      val shDept="'上海开发一部','上海开发二部','上海开发三部','上海开发四部',"
      val gzDept="'广州开发一部','广州开发二部','广州开发三部','广州开发四部','海外支持部',"
      val zhDept="'珠海开发一部','珠海开发二部','珠海开发三部','系统一部','系统二部',"
      val bjDept="'北京开发一部','北京开发二部','北京开发三部','北京开发四部','北京开发五部'"
      val acptQuestionDF=df.filter(dept+" IN ("+ hzDept +shDept +gzDept+zhDept+bjDept+")")
      acptQuestionDF
    }

  def save(procResSet:DataFrame,outputFileName:String):Unit={

  }

  def saveHdfs(procResSet:DataFrame,outputFilePath:String,outputFileName:String):Unit={
    procResSet.repartition(1).write.option("header","true")
      .csv("hdfs://"+outputFilePath+getSaveTime+"_"+outputFileName+".csv")
  }

  def saveLocal(procResSet:DataFrame,outputFilePath:String,outputFileName:String):Unit={
    procResSet.repartition(1).write.option("header","true")
      .csv("file://"+outputFilePath+getSaveTime+"_"+outputFileName+".csv")
  }

  def stop():Unit={
    sparkSession.stop()
  }
}
