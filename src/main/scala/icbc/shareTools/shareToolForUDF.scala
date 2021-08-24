package icbc.shareTools

import org.apache.spark.sql.{DataFrame, Row}

class shareToolForUDF {
  def timeaddZero(JiraDF:DataFrame):Unit={
    JiraDF.sparkSession.udf.register("addZero",(a:String)=>{
      if(a!=null){
        if(a.contains("/")){
          var c=a.toString.split("/")
          for(i<- 0 to c.size-2){
            if(c(i).toInt<10 && c(i).length < 2){
              c(i)="0"+c(i)
            }
          }
          if(c.size==3){
            val d=c(0)+"/"+c(1)+"/"+c(2)
            d
          }
        }else if(a.contains("-")){
          var c=a.toString.split("-")
          var d:String = c(0)+"/"+c(1)+"/"+c(2)
          d
        }
      }
      a
    })
  }

  def timeTransStander(JiraDF:DataFrame):Unit={
    JiraDF.sparkSession.udf.register("timeTrans",(str:String)=>{
      if(str!=null){
        if(str.contains("-")){
          val l=str.split(" ")
          val a=l(0)
          val c=a.split("-")

          for (i <- 0 to c.size-1){
            if(c(i).toInt<10 && c(i).length < 2){
              c(i)="0"+c(i)
            }
          }

          val d:String = c(0)+"/"+c(1)+"/"+c(2)
          d
        }else if(str.contains("/")){
          val temp = str.split(" ")
          val a=temp(0)
          var c=a.split("/")

          for(i <- 0 to c.size-1){
            if(c(i).toInt<10 && c(i).length < 2){
              c(i)="0"+c(i)
            }
          }

          val d:String = c(0)+"/"+c(1)+"/"+c(2)
          d
        }else{
          str
        }
      }else{
        str
      }
    })
  }

  def listToSetStrings(list:List[Row]):String={
    var stepSet:String=""
    for(i <- 0 to list.size-1 ){
      if(i!=0){
        stepSet=stepSet+","+"'"+(list(i)(0).toString)+"'"
      }else{
        stepSet=stepSet+"'"+(list(i)(0).toString)+"'"
      }
    }
    stepSet
  }

  def listToSetString(list:List[Row]):String={
    var stepSet:String=""
    for(i <- 0 to list.size-1 ){
      if(i!=0){
        stepSet=stepSet+","+"'"+(list(i)(0).toString)+"'"
      }else{
        stepSet=stepSet+"'"+(list(i)(0).toString)+"'"
      }
    }
    stepSet
  }

  def proselect(ListDF:DataFrame,func:(List[Row])=>String):List[String]={
    val confid:List[String]=" projType LIKE ('%全行重点%') OR projType LIKE ('%专业重点%') OR projType LIKE ('%IT架构转型%')"::"projType!='否'"::Nil

    val psfiled=ListDF.select("projNo","projType").filter(confid(0)).collect().toList
    val psfiled1=ListDF.select("projNo","projType").filter(confid(1)).collect().toList

    val stepSet:String=" WHERE projNo IN ("+func(psfiled)+")"
    val stepSet1:String=" WHERE projNo IN ("+func(psfiled1)+")"
    val stepSet2:String = ""
    val conditionList=stepSet::stepSet1::stepSet2::Nil
    conditionList
  }
}
