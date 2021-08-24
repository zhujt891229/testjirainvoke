package icbc.task

import java.text.SimpleDateFormat

import icbc.put.PublicUtilTools
import icbc.shareTools.shareToolForUDF
import org.apache.spark.sql.{DataFrame, Row}

class TimePeriod {
  val dateFormat = new SimpleDateFormat("yyyy/MM/dd")
  val dateFormat1 = new SimpleDateFormat("yyyy-MM-dd HH:mm")
  val sharetool = new shareToolForUDF

  def funcDemaAnalWl(personDF:DataFrame,JiraDF:DataFrame,psDF:DataFrame,ListDF:DataFrame,PUT:PublicUtilTools,startDate:String,overDate:String,func:List[Row]=>String,condition:String):DataFrame={

    sharetool.timeaddZero(JiraDF);
    sharetool.timeTransStander(JiraDF)

    val psfiled=psDF.filter(psDF("需求分析是否完成").contains("是")).collect().toList
    var stepSet:String=func(psfiled)

    val npsfiled=psDF.filter(psDF("需求分析是否完成").contains("否")).collect().toList
    var nstepSet:String=func(npsfiled)

    JiraDF.createOrReplaceTempView("demand")

    val midResult=JiraDF.sparkSession.sql(" SELECT dept,team,version,projName,projNo,appShortName," +
      "fpd,designer,issueStatus,issueType,entireManageTaskClass,personInCharge," +
      "addZero(actualfinishtime) as actualfinishtime,addZero(planFinishDate) AS planFinishDate, " +
      "timeTrans(TRIM(dueDate)) as dueDate " +
      "FROM demand")
    personDF.createOrReplaceTempView("person")
    midResult.createOrReplaceTempView("midresult")
    val midDemandResult=midResult.sparkSession.sql(" SELECT person.user,midresult.dept,midresult.team,midresult.version," +
      "midresult.projName,midresult.projNo,midresult.appShortName,midresult.fpd" +
      " FROM midresult,person " +
      " WHERE (midresult.issueStatus IN ("+stepSet+") AND (midresult.issueType = '需求项' AND midresult.designer!='0' " +
      " AND person.user=midresult.designer AND person.dept=midresult.dept AND person.team=midresult.team AND person.version=midresult.version AND person.projName=midresult.projName AND person.projNo=midresult.projNo AND person.appShortName=midresult.appShortName AND ((midresult.actualfinishtime is not null and midresult.actualfinishtime>='" +startDate+"' AND midresult.actualfinishtime<='"+overDate+"' ) OR ( midresult.actualfinishtime is null and midresult.dueDate>= '"+startDate+"' and midresult.dueDate<='"+overDate+"' ))) or (midresult.issueType='整体管理任务' and midresult.entireManageTaskClass='需求分析' and midresult.personInCharge!='0'  "+
      " and person.user=midresult.personInCharge and person.dept=midresult.dept and person.team = midresult.team and person.version=midresult.version and person.projName=midresult.projName and person.projNo=midresult.projNo and person.appShortName=midresult.appShortName and ((midresult.actualfinishtime is not null and midresult.actualfinishtime>='"+startDate+"' and midresult.actualfinishtime<='"+overDate+"') or ( midresult.actualfinishtime is null and midresult.planFinishDate>= '"+startDate+"' and midresult.planFinishDate<='"+overDate+"' ))))"+
      //" AND midresult.designActualEndDate IS NOT NULL AND midresult.designActualEndDate>= '"+startDate+"' AND midresult.designActualEndDate<='"+overDate+"' " +
      " OR (midresult.issueStatus IN ("+nstepSet+") AND (midresult.issueType = '需求项' AND  midresult.designer!='0' AND person.user=midresult.designer AND person.dept=midresult.dept AND person.team=midresult.team AND person.version=midresult.version AND person.projName=midresult.projName AND person.projNo=midresult.projNo AND person.appShortName=midresult.appShortName " +
      " and midresult.dueDate is not null and midresult.dueDate>= '" +startDate+"' and midresult.dueDate<= '"+overDate+"' ) or (midresult.issueType='整体管理任务' and midresult.personInCharge!='0' and midresult.entireManageTaskClass='需求分析' and person.user=midresult.personInCharge and person.dept=midresult.dept " +
      " and person.team=midresult.team and person.version=midresult.version and person.projName=midresult.projName and person.projNo=midresult.projNo and person.appShortName=midresult.appShortName " +
      " and midresult.planFinishDate is not null and midresult.planFinishDate>='"+startDate+"' and midresult.planFinishDate<='"+overDate+"' ))"+
      " ORDER BY dept")

      .groupBy("user","dept","team","version","projName","projNo","appShortName")
      .agg(Map("fpd"->"sum"))
      .withColumnRenamed("sum(fpd)","DemaAnalWl")

    midDemandResult.createOrReplaceTempView("prodemand")
    val demandResult=midDemandResult.sparkSession.sql(" SELECT * FROM prodemand "+condition)
    demandResult
  }

  def funcDesignWl(personDF:DataFrame,JiraDF:DataFrame,psDF:DataFrame,ListDF:DataFrame,PUT:PublicUtilTools,startDate:String,overDate:String,func:List[Row]=>String,condition:String):DataFrame={
    sharetool.timeaddZero(JiraDF);

    val psfiled=psDF.filter(psDF("设计是否完成").contains("是")).collect().toList
    var stepSet:String=func(psfiled)

    val npsfiled=psDF.filter(psDF("设计是否完成").contains("否")).collect().toList
    var nstepSet:String=func(npsfiled)

    JiraDF.createOrReplaceTempView("design")

    val midResult=JiraDF.sparkSession.sql(" SELECT dept,team,version,projName,projNo,appShortName,designFpd,designer,issueType,issueStatus,personInCharge,addZero(designActualEndDate) as designActualEndDate,addZero(designEndDate) AS designEndDate FROM design")
    personDF.createOrReplaceTempView("person")
    midResult.createOrReplaceTempView("midresult")
    val midDesignResult=midResult.sparkSession.sql(" SELECT person.user,midresult.dept,midresult.team,midresult.version,midresult.projName,midresult.projNo,midresult.appShortName,midresult.designFpd" +
      " FROM midresult,person " +
      " WHERE midresult.issueStatus IN ("+stepSet+") AND midresult.issueType LIKE ('%测试%') AND midresult.designer!='0' AND person.user=midresult.designer AND person.dept=midresult.dept AND person.team=midresult.team AND person.version=midresult.version AND person.projName=midresult.projName AND person.projNo=midresult.projNo AND person.appShortName=midresult.appShortName AND midresult.designActualEndDate IS NOT NULL AND midresult.designActualEndDate>= '"+startDate+"' AND midresult.designActualEndDate<='"+overDate+"' " +
      " OR (midresult.issueStatus IN ("+nstepSet+") AND midresult.issueType LIKE ('%测试%') AND  midresult.designer!='0' AND person.user=midresult.designer AND person.dept=midresult.dept AND person.team=midresult.team AND person.version=midresult.version AND person.projName=midresult.projName AND person.projNo=midresult.projNo AND person.appShortName=midresult.appShortName AND midresult.designEndDate IS NOT NULL AND midresult.designEndDate>= '"+startDate+"' AND midresult.designEndDate<='"+overDate+"' " +
      " ) ORDER BY dept")

      .groupBy("user","dept","team","version","projName","projNo","appShortName")
      .agg(Map("designFpd"->"sum"))
      .withColumnRenamed("sum(designFpd)","DesignWl")

    midDesignResult.createOrReplaceTempView("prodesign")
    val designResult=midDesignResult.sparkSession.sql(" SELECT * FROM prodesign "+condition)
    designResult
  }


  def funcCodingWl(personDF:DataFrame,JiraDF:DataFrame,psDF:DataFrame,ListDF:DataFrame,PUT:PublicUtilTools,startDate:String,overDate:String,func:List[Row]=>String,condition:String):DataFrame={
    sharetool.timeaddZero(JiraDF);

    val psfiled=psDF.filter(psDF("编码是否完成").contains("是")).collect().toList
    var stepSet:String=func(psfiled)

    val npsfiled=psDF.filter(psDF("编码是否完成").contains("否")).collect().toList
    var nstepSet:String=func(npsfiled)

    JiraDF.createOrReplaceTempView("code")

    val midResult=JiraDF.sparkSession.sql(" SELECT dept,team,version,projName,projNo,appShortName,programFpd,programer,issueType,issueStatus,personInCharge,addZero(programActualEndDate) AS programActualEndDate,addZero(programEndDate) as programEndDate,InterFunctionFpd FROM code")
    personDF.createOrReplaceTempView("person")
    midResult.createOrReplaceTempView("midresult")
    val midCodeResult1=midResult.sparkSession.sql(" SELECT person.user,midresult.dept,midresult.team,midresult.version,midresult.projName,midresult.projNo,midresult.appShortName,midresult.programFpd,midresult.InterFunctionFpd " +
      " FROM person,midresult " +
      " WHERE midresult.issueStatus IN ("+stepSet+") AND midresult.issueType LIKE ('%测试%') AND midresult.programer!='0' AND person.user=midresult.programer AND person.dept=midresult.dept AND person.team=midresult.team AND person.version=midresult.version AND person.projName=midresult.projName AND person.projNo=midresult.projNo AND person.appShortName=midresult.appShortName AND midresult.programActualEndDate IS NOT NULL AND midresult.programActualEndDate>= '"+startDate+"' AND midresult.programActualEndDate<='"+overDate+"' " +
      " OR (midresult.issueStatus IN ("+nstepSet+") AND midresult.issueType LIKE ('%测试%') AND  midresult.programer!='0' AND person.user=midresult.programer AND person.dept=midresult.dept AND person.team=midresult.team AND person.version=midresult.version AND person.projName=midresult.projName AND person.projNo=midresult.projNo AND person.appShortName=midresult.appShortName AND midresult.programEndDate IS NOT NULL AND midresult.programEndDate>= '"+startDate+"' AND midresult.programEndDate<='"+overDate+"' " +
      " ) ORDER BY dept")

      .groupBy("user","dept","team","version","projName","projNo","appShortName")
      .agg(Map("programFpd"->"sum","InterFunctionFpd"->"sum"))
//      .withColumnRenamed("sum(programFpd)","DesignWl")
    val noNullResultDf=midCodeResult1.na.fill(0,Array("sum(programFpd)","sum(InterFunctionFpd)"))
    val midCodeResult=noNullResultDf.withColumn("CodingWl",noNullResultDf("sum(programFpd)")+noNullResultDf("sum(InterFunctionFpd)"))
    midCodeResult.createOrReplaceTempView("procode")
    val codeResult=midCodeResult1.sparkSession.sql(" SELECT * FROM procode "+condition)
    codeResult
  }


  def funcFuncTestWl(personDF:DataFrame,JiraDF:DataFrame,psDF:DataFrame,ListDF:DataFrame,PUT:PublicUtilTools,startDate:String,overDate:String,func:List[Row]=>String,condition:String):DataFrame={
    sharetool.timeaddZero(JiraDF);

    val psfiled=psDF.filter(psDF("功能测试是否完成").contains("是")).collect().toList
    var stepSet:String=func(psfiled)

    val npsfiled=psDF.filter(psDF("功能测试是否完成").contains("否")).collect().toList
    var nstepSet:String=func(npsfiled)

    JiraDF.createOrReplaceTempView("functiontest")

    val midResult=JiraDF.sparkSession.sql(" SELECT dept,team,version,projName,projNo,appShortName,testFpd,tester,issueType,issueStatus,personInCharge,addZero(testActualEndDate) as testActualEndDate,addZero(testEndDate) AS testEndDate FROM functiontest")
    personDF.createOrReplaceTempView("person")
    midResult.createOrReplaceTempView("midresult")
    val midDesignResult=midResult.sparkSession.sql(" SELECT person.user,midresult.dept,midresult.team,midresult.version,midresult.projName,midresult.projNo,midresult.appShortName,midresult.testFpd" +
      " FROM midresult,person " +
      " WHERE midresult.issueStatus IN ("+stepSet+") AND midresult.issueType LIKE ('%测试%') AND midresult.tester!='0' AND person.user=midresult.tester AND person.dept=midresult.dept AND person.team=midresult.team AND person.version=midresult.version AND person.projName=midresult.projName AND person.projNo=midresult.projNo AND person.appShortName=midresult.appShortName AND midresult.testActualEndDate IS NOT NULL AND midresult.testActualEndDate>= '"+startDate+"' AND midresult.testActualEndDate<='"+overDate+"' " +
      " OR (midresult.issueStatus IN ("+nstepSet+") AND midresult.issueType LIKE ('%测试%') AND  midresult.tester!='0' AND person.user=midresult.tester AND person.dept=midresult.dept AND person.team=midresult.team AND person.version=midresult.version AND person.projName=midresult.projName AND person.projNo=midresult.projNo AND person.appShortName=midresult.appShortName AND midresult.testEndDate IS NOT NULL AND midresult.testEndDate>= '"+startDate+"' AND midresult.testEndDate<='"+overDate+"' " +
      " ) ORDER BY dept")

      .groupBy("user","dept","team","version","projName","projNo","appShortName")
      .agg(Map("testFpd"->"sum"))
      .withColumnRenamed("sum(testFpd)","FuncTestWl")

    midDesignResult.createOrReplaceTempView("profunctest")
    val funcTestResult=midDesignResult.sparkSession.sql(" SELECT * FROM profunctest "+condition)
    funcTestResult
  }

  def funcFloTestWl(personDF:DataFrame,JiraDF:DataFrame,psDF:DataFrame,ListDF:DataFrame,PUT:PublicUtilTools,startDate:String,overDate:String,func:List[Row]=>String,condition:String):DataFrame={
    sharetool.timeaddZero(JiraDF);
    sharetool.timeTransStander(JiraDF)

    val psfiled=psDF.filter(psDF("流程测试是否完成").contains("是")).collect().toList
    var stepSet:String=func(psfiled)

    val npsfiled=psDF.filter(psDF("流程测试是否完成").contains("否")).collect().toList
    var nstepSet:String=func(npsfiled)

    JiraDF.createOrReplaceTempView("flowtest")

    val midResult=JiraDF.sparkSession.sql(" SELECT dept,team,version,projName,projNo,appShortName,flowTestFpd,flowTester,issueType,issueStatus,personInCharge,timeTrans(flowTestActualEndTime) as flowTestActualEndTime,timeTrans(flowTestEndTime) AS flowTestEndTime FROM flowtest")
    personDF.createOrReplaceTempView("person")
    midResult.createOrReplaceTempView("midresult")
    val midFloTestResult=midResult.sparkSession.sql(" SELECT person.user,midresult.dept,midresult.team,midresult.version,midresult.projName,midresult.projNo,midresult.appShortName,midresult.flowTestFpd" +
      " FROM midresult,person " +
      " WHERE midresult.issueStatus IN ("+stepSet+") AND (midresult.issueType LIKE ('%测试%') OR midresult.issueType = '业务场景任务') AND midresult.flowTester!='0' AND person.user=midresult.flowTester AND person.dept=midresult.dept AND person.team=midresult.team AND person.version=midresult.version AND person.projName=midresult.projName AND person.projNo=midresult.projNo AND person.appShortName=midresult.appShortName AND midresult.flowTestActualEndTime IS NOT NULL AND midresult.flowTestActualEndTime>= '"+startDate+"' AND midresult.flowTestActualEndTime<='"+overDate+"' " +
      "  OR (midresult.issueStatus IN ("+nstepSet+") AND (midresult.issueType LIKE ('%测试%') OR midresult.issueType = '业务场景任务') AND midresult.flowTester!='0' AND person.user=midresult.flowTester AND person.dept=midresult.dept AND person.team=midresult.team AND person.version=midresult.version AND person.projName=midresult.projName AND person.projNo=midresult.projNo AND person.appShortName=midresult.appShortName AND midresult.flowTestEndTime IS NOT NULL AND midresult.flowTestEndTime>= '"+startDate+"' AND midresult.flowTestEndTime<='"+overDate+"' " +
      " ) ORDER BY dept")

      .groupBy("user","dept","team","version","projName","projNo","appShortName")
      .agg(Map("flowTestFpd"->"sum"))
      .withColumnRenamed("sum(flowTestFpd)","FloTestWl")

    midFloTestResult.createOrReplaceTempView("proflotest")
    val floTestResult=midFloTestResult.sparkSession.sql(" SELECT * FROM proflotest "+condition)
    floTestResult
  }

  def funcAutoUnitTestWl(personDF:DataFrame,JiraDF:DataFrame,psDF:DataFrame,ListDF:DataFrame,PUT:PublicUtilTools,startDate:String,overDate:String,func:List[Row]=>String,condition:String,acDate:String,plDate:String):DataFrame={
    sharetool.timeaddZero(JiraDF);

    val psfiled=psDF.filter(psDF("需求分析是否完成").contains("是")).collect().toList
    var stepSet:String=func(psfiled)

    val npsfiled=psDF.filter(psDF("需求分析是否完成").contains("否")).collect().toList
    var nstepSet:String=func(npsfiled)

    JiraDF.createOrReplaceTempView("autounittest")

    val midResult=JiraDF.sparkSession.sql(" SELECT dept,team,version,projName,projNo,appShortName,fpd,issueType,issueStatus,entireManageTaskClass,AutoTestClass,personInCharge,addZero(actualfinishtime) as actualfinishtime,addZero(planFinishDate) AS planFinishDate FROM autounittest")
    personDF.createOrReplaceTempView("person")
    midResult.createOrReplaceTempView("midresult")
    val midDesignResult=midResult.sparkSession.sql(" SELECT person.user,midresult.dept,midresult.team,midresult.version,midresult.projName,midresult.projNo,midresult.appShortName,midresult.fpd" +
      " FROM midresult,person " +
      " WHERE midresult.issueStatus IN ("+stepSet+") AND midresult.issueType = '整体管理任务' AND midresult.personInCharge !='0' AND midresult.entireManageTaskClass='自动化测试' AND midresult.AutoTestClass='单元测试' AND person.user=midresult.personInCharge AND person.dept=midresult.dept AND person.team=midresult.team AND person.version=midresult.version AND person.projName=midresult.projName AND person.projNo=midresult.projNo AND person.appShortName=midresult.appShortName AND "+acDate +
      " OR (midresult.issueStatus IN ("+nstepSet+") AND midresult.issueType = '整体管理任务' AND midresult.personInCharge !='0' AND midresult.entireManageTaskClass='自动化测试' AND midresult.AutoTestClass='单元测试'  AND person.user=midresult.personInCharge AND person.dept=midresult.dept AND person.team=midresult.team AND person.version=midresult.version AND person.projName=midresult.projName AND person.projNo=midresult.projNo AND person.appShortName=midresult.appShortName AND "+plDate +
      " ) ORDER BY dept")

      .groupBy("user","dept","team","version","projName","projNo","appShortName")
      .agg(Map("fpd"->"sum"))
      .withColumnRenamed("sum(fpd)","AutoUnitTestWl")

    midDesignResult.createOrReplaceTempView("proautounittest")
    val designResult=midDesignResult.sparkSession.sql(" SELECT * FROM proautounittest "+condition)
    designResult
  }

  def funcAutoFuncTestWl(personDF:DataFrame,JiraDF:DataFrame,psDF:DataFrame,ListDF:DataFrame,PUT:PublicUtilTools,startDate:String,overDate:String,func:List[Row]=>String,condition:String,acDate:String,plDate:String):DataFrame={
    sharetool.timeaddZero(JiraDF);

    val psfiled=psDF.filter(psDF("需求分析是否完成").contains("是")).collect().toList
    var stepSet:String=func(psfiled)

    val npsfiled=psDF.filter(psDF("需求分析是否完成").contains("否")).collect().toList
    var nstepSet:String=func(npsfiled)

    JiraDF.createOrReplaceTempView("autofunctest")

    val midResult=JiraDF.sparkSession.sql(" SELECT dept,team,version,projName,projNo,appShortName,fpd,issueType,issueStatus,entireManageTaskClass,AutoTestClass,personInCharge,addZero(actualfinishtime) as actualfinishtime,addZero(planFinishDate) AS planFinishDate FROM autofunctest")
    personDF.createOrReplaceTempView("person")
    midResult.createOrReplaceTempView("midresult")
    val midAutoFuncTestResult=midResult.sparkSession.sql(" SELECT person.user,midresult.dept,midresult.team,midresult.version,midresult.projName,midresult.projNo,midresult.appShortName,midresult.fpd" +
      " FROM midresult,person " +
      " WHERE midresult.issueStatus IN ("+stepSet+") AND midresult.issueType = '整体管理任务' AND midresult.personInCharge !='0' AND midresult.entireManageTaskClass='自动化测试' AND midresult.AutoTestClass='功能测试' AND person.user=midresult.personInCharge AND person.dept=midresult.dept AND person.team=midresult.team AND person.version=midresult.version AND person.projName=midresult.projName AND person.projNo=midresult.projNo AND person.appShortName=midresult.appShortName AND "+acDate +
      " OR (midresult.issueStatus IN ("+nstepSet+") AND midresult.issueType = '整体管理任务' AND midresult.personInCharge !='0' AND midresult.entireManageTaskClass='自动化测试' AND midresult.AutoTestClass='功能测试'  AND person.user=midresult.personInCharge AND person.dept=midresult.dept AND person.team=midresult.team AND person.version=midresult.version AND person.projName=midresult.projName AND person.projNo=midresult.projNo AND person.appShortName=midresult.appShortName AND "+plDate +
      " ) ORDER BY dept")

      .groupBy("user","dept","team","version","projName","projNo","appShortName")
      .agg(Map("fpd"->"sum"))
      .withColumnRenamed("sum(fpd)","AutoFuncTestWl")

    midAutoFuncTestResult.createOrReplaceTempView("proautofunctest")
    val autoFuncTestResult=midAutoFuncTestResult.sparkSession.sql(" SELECT * FROM proautofunctest "+condition)
    autoFuncTestResult
  }

  def funcAutoProTestWl(personDF:DataFrame,JiraDF:DataFrame,psDF:DataFrame,ListDF:DataFrame,PUT:PublicUtilTools,startDate:String,overDate:String,func:List[Row]=>String,condition:String,acDate:String,plDate:String):DataFrame={
    sharetool.timeaddZero(JiraDF);

    val psfiled=psDF.filter(psDF("需求分析是否完成").contains("是")).collect().toList
    var stepSet:String=func(psfiled)

    val npsfiled=psDF.filter(psDF("需求分析是否完成").contains("否")).collect().toList
    var nstepSet:String=func(npsfiled)

    JiraDF.createOrReplaceTempView("autoprotest")

    val midResult=JiraDF.sparkSession.sql(" SELECT dept,team,version,projName,projNo,appShortName,fpd,issueType,issueStatus,entireManageTaskClass,AutoTestClass,personInCharge,addZero(actualfinishtime) as actualfinishtime,addZero(planFinishDate) AS planFinishDate FROM autoprotest")
    personDF.createOrReplaceTempView("person")
    midResult.createOrReplaceTempView("midresult")
    val midAutoProTestResult=midResult.sparkSession.sql(" SELECT person.user,midresult.dept,midresult.team,midresult.version,midresult.projName,midresult.projNo,midresult.appShortName,midresult.fpd" +
      " FROM midresult,person " +
      " WHERE midresult.issueStatus IN ("+stepSet+") AND midresult.issueType = '整体管理任务' AND midresult.personInCharge !='0' AND midresult.entireManageTaskClass='自动化测试' AND midresult.AutoTestClass='流程测试' AND person.user=midresult.personInCharge AND person.dept=midresult.dept AND person.team=midresult.team AND person.version=midresult.version AND person.projName=midresult.projName AND person.projNo=midresult.projNo AND person.appShortName=midresult.appShortName AND "+acDate +
      " OR (midresult.issueStatus IN ("+nstepSet+") AND midresult.issueType = '整体管理任务' AND midresult.personInCharge !='0' AND midresult.entireManageTaskClass='自动化测试' AND midresult.AutoTestClass='流程测试'  AND person.user=midresult.personInCharge AND person.dept=midresult.dept AND person.team=midresult.team AND person.version=midresult.version AND person.projName=midresult.projName AND person.projNo=midresult.projNo AND person.appShortName=midresult.appShortName AND "+plDate +
      " ) ORDER BY dept")

      .groupBy("user","dept","team","version","projName","projNo","appShortName")
      .agg(Map("fpd"->"sum"))
      .withColumnRenamed("sum(fpd)","AutoproTestWl")

    midAutoProTestResult.createOrReplaceTempView("proautoprotest")
    val autoProTestResult=midAutoProTestResult.sparkSession.sql(" SELECT * FROM proautoprotest "+condition)
    autoProTestResult
  }

  def funcProModifiWl(personDF:DataFrame,JiraDF:DataFrame,psDF:DataFrame,ListDF:DataFrame,PUT:PublicUtilTools,startDate:String,overDate:String,func:List[Row]=>String,condition:String,acDate:String,plDate:String):DataFrame={
    sharetool.timeaddZero(JiraDF);

    val psfiled=psDF.filter(psDF("需求分析是否完成").contains("是")).collect().toList
    var stepSet:String=func(psfiled)

    val npsfiled=psDF.filter(psDF("需求分析是否完成").contains("否")).collect().toList
    var nstepSet:String=func(npsfiled)

    JiraDF.createOrReplaceTempView("promodifi")

    val midResult=JiraDF.sparkSession.sql(" SELECT dept,team,version,projName,projNo,appShortName,fpd,issueType,issueStatus,entireManageTaskClass,workType,personInCharge,addZero(actualfinishtime) as actualfinishtime,addZero(planFinishDate) AS planFinishDate FROM promodifi")
    personDF.createOrReplaceTempView("person")
    midResult.createOrReplaceTempView("midresult")
    val midProModifiResult=midResult.sparkSession.sql(" SELECT person.user,midresult.dept,midresult.team,midresult.version,midresult.projName,midresult.projNo,midresult.appShortName,midresult.fpd" +
      " FROM midresult,person " +
      " WHERE midresult.issueStatus IN ("+stepSet+") AND midresult.issueType = '整体管理任务' AND midresult.personInCharge !='0' AND midresult.entireManageTaskClass='问题修改' AND person.user=midresult.personInCharge AND person.dept=midresult.dept AND person.team=midresult.team AND person.version=midresult.version AND person.projName=midresult.projName AND person.projNo=midresult.projNo AND person.appShortName=midresult.appShortName AND "+acDate +
      " OR (midresult.issueStatus IN ("+nstepSet+") AND midresult.issueType = '整体管理任务' AND midresult.personInCharge !='0' AND midresult.entireManageTaskClass='问题修改' AND person.user=midresult.personInCharge AND person.dept=midresult.dept AND person.team=midresult.team AND person.version=midresult.version AND person.projName=midresult.projName AND person.projNo=midresult.projNo AND person.appShortName=midresult.appShortName AND "+plDate +
      " ) ORDER BY dept")

      .groupBy("user","dept","team","version","projName","projNo","appShortName")
      .agg(Map("fpd"->"sum"))
      .withColumnRenamed("sum(fpd)","ProModifiWl")

    midProModifiResult.createOrReplaceTempView("propromodifi")
    val proModifiResult=midProModifiResult.sparkSession.sql(" SELECT * FROM propromodifi "+condition)
    proModifiResult
  }

  def funcCodeRevWl(personDF:DataFrame,JiraDF:DataFrame,psDF:DataFrame,ListDF:DataFrame,PUT:PublicUtilTools,startDate:String,overDate:String,func:List[Row]=>String,condition:String,acDate:String,plDate:String):DataFrame={
    sharetool.timeaddZero(JiraDF);

    val psfiled=psDF.filter(psDF("需求分析是否完成").contains("是")).collect().toList
    var stepSet:String=func(psfiled)

    val npsfiled=psDF.filter(psDF("需求分析是否完成").contains("否")).collect().toList
    var nstepSet:String=func(npsfiled)

    JiraDF.createOrReplaceTempView("codereview")

    val midResult=JiraDF.sparkSession.sql(" SELECT dept,team,version,projName,projNo,appShortName,fpd,issueType,issueStatus,entireManageTaskClass,workType,personInCharge,addZero(actualfinishtime) as actualfinishtime,addZero(planFinishDate) AS planFinishDate FROM codereview")
    personDF.createOrReplaceTempView("person")
    midResult.createOrReplaceTempView("midresult")
    val midCodRevResult=midResult.sparkSession.sql(" SELECT person.user,midresult.dept,midresult.team,midresult.version,midresult.projName,midresult.projNo,midresult.appShortName,midresult.fpd" +
      " FROM midresult,person " +
      " WHERE midresult.issueStatus IN ("+stepSet+") AND midresult.issueType = '整体管理任务' AND midresult.personInCharge !='0' AND midresult.entireManageTaskClass='代码复核' AND person.user=midresult.personInCharge AND person.dept=midresult.dept AND person.team=midresult.team AND person.version=midresult.version AND person.projName=midresult.projName AND person.projNo=midresult.projNo AND person.appShortName=midresult.appShortName AND "+acDate +
      " OR (midresult.issueStatus IN ("+nstepSet+") AND midresult.issueType = '整体管理任务' AND midresult.personInCharge !='0' AND midresult.entireManageTaskClass='代码复核' AND person.user=midresult.personInCharge AND person.dept=midresult.dept AND person.team=midresult.team AND person.version=midresult.version AND person.projName=midresult.projName AND person.projNo=midresult.projNo AND person.appShortName=midresult.appShortName AND "+plDate +
      " ) ORDER BY dept")

      .groupBy("user","dept","team","version","projName","projNo","appShortName")
      .agg(Map("fpd"->"sum"))
      .withColumnRenamed("sum(fpd)","CodeRevWl")

    midCodRevResult.createOrReplaceTempView("procodrev")
    val codRevResult=midCodRevResult.sparkSession.sql(" SELECT * FROM procodrev "+condition)
    codRevResult
  }

  def funcOverDesWl(personDF:DataFrame,JiraDF:DataFrame,psDF:DataFrame,ListDF:DataFrame,PUT:PublicUtilTools,startDate:String,overDate:String,func:List[Row]=>String,condition:String,acDate:String,plDate:String):DataFrame={
    sharetool.timeaddZero(JiraDF);

    val psfiled=psDF.filter(psDF("需求分析是否完成").contains("是")).collect().toList
    var stepSet:String=func(psfiled)

    val npsfiled=psDF.filter(psDF("需求分析是否完成").contains("否")).collect().toList
    var nstepSet:String=func(npsfiled)

    JiraDF.createOrReplaceTempView("overalldesign")

    val midResult=JiraDF.sparkSession.sql(" SELECT dept,team,version,projName,projNo,appShortName,fpd,issueType,issueStatus,entireManageTaskClass,workType,personInCharge,addZero(actualfinishtime) as actualfinishtime,addZero(planFinishDate) AS planFinishDate FROM overalldesign")
    personDF.createOrReplaceTempView("person")
    midResult.createOrReplaceTempView("midresult")
    val midOverDesResult=midResult.sparkSession.sql(" SELECT person.user,midresult.dept,midresult.team,midresult.version,midresult.projName,midresult.projNo,midresult.appShortName,midresult.fpd" +
      " FROM midresult,person " +
      " WHERE midresult.issueStatus IN ("+stepSet+") AND midresult.issueType = '整体管理任务' AND midresult.personInCharge !='0' AND midresult.entireManageTaskClass='整体设计' AND midresult.workType='总体设计' AND person.user=midresult.personInCharge AND person.dept=midresult.dept AND person.team=midresult.team AND person.version=midresult.version AND person.projName=midresult.projName AND person.projNo=midresult.projNo AND person.appShortName=midresult.appShortName AND "+acDate +
      " OR (midresult.issueStatus IN ("+nstepSet+") AND midresult.issueType = '整体管理任务' AND midresult.personInCharge !='0' AND midresult.entireManageTaskClass='整体设计' AND midresult.workType='总体设计' AND person.user=midresult.personInCharge AND person.dept=midresult.dept AND person.team=midresult.team AND person.version=midresult.version AND person.projName=midresult.projName AND person.projNo=midresult.projNo AND person.appShortName=midresult.appShortName AND "+plDate +
      " ) ORDER BY dept")

      .groupBy("user","dept","team","version","projName","projNo","appShortName")
      .agg(Map("fpd"->"sum"))
      .withColumnRenamed("sum(fpd)","OverDesWl")

    midOverDesResult.createOrReplaceTempView("prooverdes")
    val overDesResult=midOverDesResult.sparkSession.sql(" SELECT * FROM prooverdes "+condition)
    overDesResult
  }

  def funcTestDesWl(personDF:DataFrame,JiraDF:DataFrame,psDF:DataFrame,ListDF:DataFrame,PUT:PublicUtilTools,startDate:String,overDate:String,func:List[Row]=>String,condition:String,acDate:String,plDate:String):DataFrame={
    sharetool.timeaddZero(JiraDF);

    val psfiled=psDF.filter(psDF("需求分析是否完成").contains("是")).collect().toList
    var stepSet:String=func(psfiled)

    val npsfiled=psDF.filter(psDF("需求分析是否完成").contains("否")).collect().toList
    var nstepSet:String=func(npsfiled)

    JiraDF.createOrReplaceTempView("testdesign")

    val midResult=JiraDF.sparkSession.sql(" SELECT dept,team,version,projName,projNo,appShortName,fpd,flowTestDesignFpd,flowTestDesigner,issueType,issueStatus,entireManageTaskClass,workType,personInCharge,addZero(actualfinishtime) as actualfinishtime,addZero(planFinishDate) AS planFinishDate,addZero(flowTestDesignActualFinishTime) as flowTestDesignActualFinishTime,addZero(flowTestDesignPlanFinishTime) as flowTestDesignPlanFinishTime FROM testdesign")
    personDF.createOrReplaceTempView("person")
    midResult.createOrReplaceTempView("midresult")

    val plDateRule2:String=" midresult.flowTestDesignPlanFinishTime IS NOT NULL AND midresult.flowTestDesignPlanFinishTime>='"+startDate+"' AND midresult.flowTestDesignPlanFinishTime<='"+overDate+"' "
    val acDateRule2:String=" ((midresult.flowTestDesignActualFinishTime IS NOT NULL AND midresult.flowTestDesignActualFinishTime>='"+startDate+"' AND midresult.flowTestDesignActualFinishTime<='"+overDate+"' ) OR (midresult.flowTestDesignActualFinishTime IS NULL AND midresult.flowTestDesignPlanFinishTime>='"+startDate+"' AND midresult.flowTestDesignPlanFinishTime<='"+overDate+"'))"

    val midTestDesResult1=midResult.sparkSession.sql(" SELECT person.user,midresult.dept,midresult.team,midresult.version,midresult.projName,midresult.projNo,midresult.appShortName,midresult.fpd,midresult.flowTestDesignFpd,midresult.flowTestDesigner" +
      " FROM midresult,person " +
      " WHERE midresult.issueStatus IN ("+stepSet+") AND midresult.issueType = '流程测试设计任务' AND midresult.personInCharge !='0' AND person.user=midresult.personInCharge AND person.dept=midresult.dept AND person.team=midresult.team AND person.version=midresult.version AND person.projName=midresult.projName AND person.projNo=midresult.projNo AND person.appShortName=midresult.appShortName AND "+plDateRule2 +
      " OR (midresult.issueStatus IN ("+nstepSet+") AND midresult.issueType = '流程测试设计任务' AND midresult.personInCharge !='0' AND person.user=midresult.personInCharge AND person.dept=midresult.dept AND person.team=midresult.team AND person.version=midresult.version AND person.projName=midresult.projName AND person.projNo=midresult.projNo AND person.appShortName=midresult.appShortName AND "+acDateRule2 +
      " ) ORDER BY dept")

      .groupBy("user","dept","team","version","projName","projNo","appShortName")
      .agg(Map("flowTestDesignFpd"->"sum"))
      .withColumnRenamed("sum(flowTestDesignFpd)","TestDesWl")

    val midTestDesResult2=midResult.sparkSession.sql(" SELECT person.user,midresult.dept,midresult.team,midresult.version,midresult.projName,midresult.projNo,midresult.appShortName,midresult.fpd" +
      " FROM midresult,person " +
      " WHERE midresult.issueStatus IN ("+stepSet+") AND midresult.issueType = '整体管理任务' AND midresult.personInCharge !='0' AND midresult.entireManageTaskClass='测试设计' AND person.user=midresult.personInCharge AND person.dept=midresult.dept AND person.team=midresult.team AND person.version=midresult.version AND person.projName=midresult.projName AND person.projNo=midresult.projNo AND person.appShortName=midresult.appShortName AND "+acDate +
      " OR (midresult.issueStatus IN ("+nstepSet+") AND midresult.issueType = '整体管理任务' AND midresult.personInCharge !='0' AND midresult.entireManageTaskClass='测试设计' AND person.user=midresult.personInCharge AND person.dept=midresult.dept AND person.team=midresult.team AND person.version=midresult.version AND person.projName=midresult.projName AND person.projNo=midresult.projNo AND person.appShortName=midresult.appShortName AND "+plDate +
      " ) ORDER BY dept")
      .groupBy("user","dept","team","version","projName","projNo","appShortName")
      .agg(Map("fpd"->"sum"))
      .withColumnRenamed("sum(fpd)","TestDesWl")

    val midTestDesReult = midTestDesResult1.union(midTestDesResult2)

    midTestDesReult.createOrReplaceTempView("protestdes")
    val testDesResult=midTestDesResult1.sparkSession.sql(" SELECT * FROM protestdes "+condition)
    testDesResult
  }


  def funcProgramDesWl(personDF:DataFrame,JiraDF:DataFrame,psDF:DataFrame,ListDF:DataFrame,PUT:PublicUtilTools,startDate:String,overDate:String,func:List[Row]=>String,condition:String,acDate:String,plDate:String):DataFrame={
    sharetool.timeaddZero(JiraDF);

    val psfiled=psDF.filter(psDF("需求分析是否完成").contains("是")).collect().toList
    var stepSet:String=func(psfiled)

    val npsfiled=psDF.filter(psDF("需求分析是否完成").contains("否")).collect().toList
    var nstepSet:String=func(npsfiled)

    JiraDF.createOrReplaceTempView("programdesign")

    val midResult=JiraDF.sparkSession.sql(" SELECT dept,team,version,projName,projNo,appShortName,fpd,issueType,issueStatus,entireManageTaskClass,workType,personInCharge,addZero(actualfinishtime) as actualfinishtime,addZero(planFinishDate) AS planFinishDate FROM programdesign")
    personDF.createOrReplaceTempView("person")
    midResult.createOrReplaceTempView("midresult")
    val midProgramDesResult=midResult.sparkSession.sql(" SELECT person.user,midresult.dept,midresult.team,midresult.version,midresult.projName,midresult.projNo,midresult.appShortName,midresult.fpd" +
      " FROM midresult,person " +
      " WHERE midresult.issueStatus IN ("+stepSet+") AND midresult.issueType = '整体管理任务' AND midresult.personInCharge !='0' AND midresult.entireManageTaskClass='整体设计' AND midresult.workType='程序设计' AND person.user=midresult.personInCharge AND person.dept=midresult.dept AND person.team=midresult.team AND person.version=midresult.version AND person.projName=midresult.projName AND person.projNo=midresult.projNo AND person.appShortName=midresult.appShortName AND "+acDate +
      " OR (midresult.issueStatus IN ("+nstepSet+") AND midresult.issueType = '整体管理任务' AND midresult.personInCharge !='0' AND midresult.entireManageTaskClass='整体设计' AND midresult.workType='程序设计' AND person.user=midresult.personInCharge AND person.dept=midresult.dept AND person.team=midresult.team AND person.version=midresult.version AND person.projName=midresult.projName AND person.projNo=midresult.projNo AND person.appShortName=midresult.appShortName AND "+plDate +
      " ) ORDER BY dept")

      .groupBy("user","dept","team","version","projName","projNo","appShortName")
      .agg(Map("fpd"->"sum"))
      .withColumnRenamed("sum(fpd)","ProgramDesWl")

    midProgramDesResult.createOrReplaceTempView("proprogramdes")
    val programDesResult=midProgramDesResult.sparkSession.sql(" SELECT * FROM proprogramdes "+condition)
    programDesResult
  }

  def funcCoordTestWl(personDF:DataFrame,JiraDF:DataFrame,psDF:DataFrame,ListDF:DataFrame,PUT:PublicUtilTools,startDate:String,overDate:String,func:List[Row]=>String,condition:String,acDate:String,plDate:String):DataFrame={
    sharetool.timeaddZero(JiraDF);

    val psfiled=psDF.filter(psDF("需求分析是否完成").contains("是")).collect().toList
    var stepSet:String=func(psfiled)

    val npsfiled=psDF.filter(psDF("需求分析是否完成").contains("否")).collect().toList
    var nstepSet:String=func(npsfiled)

    JiraDF.createOrReplaceTempView("coordinatetest")

    val midResult=JiraDF.sparkSession.sql(" SELECT dept,team,version,projName,projNo,appShortName,fpd,issueType,issueStatus,entireManageTaskClass,workType,personInCharge,addZero(actualfinishtime) as actualfinishtime,addZero(planFinishDate) AS planFinishDate FROM otheroverdesign")
    personDF.createOrReplaceTempView("person")
    midResult.createOrReplaceTempView("midresult")
    val midCoordTestResult=midResult.sparkSession.sql(" SELECT person.user,midresult.dept,midresult.team,midresult.version,midresult.projName,midresult.projNo,midresult.appShortName,midresult.fpd" +
      " FROM midresult,person " +
      " WHERE midresult.issueStatus IN ("+stepSet+") AND midresult.issueType = '整体管理任务' AND midresult.personInCharge !='0' AND (midresult.entireManageTaskClass='配合测试' OR midresult.entireManageTaskClass='性能测试' OR midresult.entireManageTaskClass='专项测试') AND person.user=midresult.personInCharge AND person.dept=midresult.dept AND person.team=midresult.team AND person.version=midresult.version AND person.projName=midresult.projName AND person.projNo=midresult.projNo AND person.appShortName=midresult.appShortName AND "+acDate +
      " OR (midresult.issueStatus IN ("+nstepSet+") AND midresult.issueType = '整体管理任务' AND midresult.personInCharge !='0' AND (midresult.entireManageTaskClass='配合测试' OR midresult.entireManageTaskClass='性能测试' OR midresult.entireManageTaskClass='专项测试') AND person.user=midresult.personInCharge AND person.dept=midresult.dept AND person.team=midresult.team AND person.version=midresult.version AND person.projName=midresult.projName AND person.projNo=midresult.projNo AND person.appShortName=midresult.appShortName AND "+plDate +
      " ) ORDER BY dept")

      .groupBy("user","dept","team","version","projName","projNo","appShortName")
      .agg(Map("fpd"->"sum"))
      .withColumnRenamed("sum(fpd)","CoordTestWl")

    midCoordTestResult.createOrReplaceTempView("procoordtest")
    val coordTestResult=midCoordTestResult.sparkSession.sql(" SELECT * FROM procoordtest "+condition)
    coordTestResult
  }

  def funcOtherOverDesWl(personDF:DataFrame,JiraDF:DataFrame,psDF:DataFrame,ListDF:DataFrame,PUT:PublicUtilTools,startDate:String,overDate:String,func:List[Row]=>String,condition:String,acDate:String,plDate:String):DataFrame={
    sharetool.timeaddZero(JiraDF);

    val psfiled=psDF.filter(psDF("需求分析是否完成").contains("是")).collect().toList
    var stepSet:String=func(psfiled)

    val npsfiled=psDF.filter(psDF("需求分析是否完成").contains("否")).collect().toList
    var nstepSet:String=func(npsfiled)

    JiraDF.createOrReplaceTempView("otheroverdesign")

    val midResult=JiraDF.sparkSession.sql(" SELECT dept,team,version,projName,projNo,appShortName,fpd,issueType,issueStatus,entireManageTaskClass,workType,personInCharge,addZero(actualfinishtime) as actualfinishtime,addZero(planFinishDate) AS planFinishDate FROM otheroverdesign")
    personDF.createOrReplaceTempView("person")
    midResult.createOrReplaceTempView("midresult")
    val midOtherOverDesResult=midResult.sparkSession.sql(" SELECT person.user,midresult.dept,midresult.team,midresult.version,midresult.projName,midresult.projNo,midresult.appShortName,midresult.fpd" +
      " FROM midresult,person " +
      " WHERE midresult.issueStatus IN ("+stepSet+") AND midresult.issueType = '整体管理任务' AND midresult.personInCharge !='0' AND midresult.entireManageTaskClass='整体设计' AND midresult.workType!='程序设计' AND midresult.workType!='总体设计' AND midresult.workType!='测试设计' AND person.user=midresult.personInCharge AND person.dept=midresult.dept AND person.team=midresult.team AND person.version=midresult.version AND person.projName=midresult.projName AND person.projNo=midresult.projNo AND person.appShortName=midresult.appShortName AND "+acDate +
      " OR (midresult.issueStatus IN ("+nstepSet+") AND midresult.issueType = '整体管理任务' AND midresult.personInCharge !='0' AND midresult.entireManageTaskClass='整体设计' AND midresult.workType!='程序设计' AND midresult.workType!='总体设计' AND midresult.workType!='测试设计' AND person.user=midresult.personInCharge AND person.dept=midresult.dept AND person.team=midresult.team AND person.version=midresult.version AND person.projName=midresult.projName AND person.projNo=midresult.projNo AND person.appShortName=midresult.appShortName AND "+plDate +
      " ) ORDER BY dept")

      .groupBy("user","dept","team","version","projName","projNo","appShortName")
      .agg(Map("fpd"->"sum"))
      .withColumnRenamed("sum(fpd)","OtherOverDesWl")

    midOtherOverDesResult.createOrReplaceTempView("prootheroverdes")
    val otherOverDesResult=midOtherOverDesResult.sparkSession.sql(" SELECT * FROM prootheroverdes "+condition)
    otherOverDesResult
  }

  def funcProManageWl(personDF:DataFrame,JiraDF:DataFrame,psDF:DataFrame,ListDF:DataFrame,PUT:PublicUtilTools,startDate:String,overDate:String,func:List[Row]=>String,condition:String,acDate:String,plDate:String):DataFrame={
    sharetool.timeaddZero(JiraDF);

    val psfiled=psDF.filter(psDF("需求分析是否完成").contains("是")).collect().toList
    var stepSet:String=func(psfiled)

    val npsfiled=psDF.filter(psDF("需求分析是否完成").contains("否")).collect().toList
    var nstepSet:String=func(npsfiled)

    JiraDF.createOrReplaceTempView("projectmanage")

    val midResult=JiraDF.sparkSession.sql(" SELECT dept,team,version,projName,projNo,appShortName,fpd,issueType,issueStatus,entireManageTaskClass,workType,personInCharge,addZero(actualfinishtime) as actualfinishtime,addZero(planFinishDate) AS planFinishDate FROM projectmanage")
    personDF.createOrReplaceTempView("person")
    midResult.createOrReplaceTempView("midresult")
    val midProManageResult=midResult.sparkSession.sql(" SELECT person.user,midresult.dept,midresult.team,midresult.version,midresult.projName,midresult.projNo,midresult.appShortName,midresult.fpd" +
      " FROM midresult,person " +
      " WHERE midresult.issueStatus IN ("+stepSet+") AND midresult.issueType = '整体管理任务' AND midresult.personInCharge !='0' AND midresult.entireManageTaskClass='项目管理' AND person.user=midresult.personInCharge AND person.dept=midresult.dept AND person.team=midresult.team AND person.version=midresult.version AND person.projName=midresult.projName AND person.projNo=midresult.projNo AND person.appShortName=midresult.appShortName AND "+acDate +
      " OR (midresult.issueStatus IN ("+nstepSet+") AND midresult.issueType = '整体管理任务' AND midresult.personInCharge !='0' AND midresult.entireManageTaskClass='项目管理' AND person.user=midresult.personInCharge AND person.dept=midresult.dept AND person.team=midresult.team AND person.version=midresult.version AND person.projName=midresult.projName AND person.projNo=midresult.projNo AND person.appShortName=midresult.appShortName AND "+plDate +
      " ) ORDER BY dept")

      .groupBy("user","dept","team","version","projName","projNo","appShortName")
      .agg(Map("fpd"->"sum"))
      .withColumnRenamed("sum(fpd)","ProManageWl")

    midProManageResult.createOrReplaceTempView("propromanage")
    val proManageResult=midProManageResult.sparkSession.sql(" SELECT * FROM propromanage "+condition)
    proManageResult
  }

  def funcOtherOverManageWl(personDF:DataFrame,JiraDF:DataFrame,psDF:DataFrame,ListDF:DataFrame,PUT:PublicUtilTools,startDate:String,overDate:String,func:List[Row]=>String,condition:String,acDate:String,plDate:String):DataFrame={
    sharetool.timeaddZero(JiraDF);

    val psfiled=psDF.filter(psDF("需求分析是否完成").contains("是")).collect().toList
    var stepSet:String=func(psfiled)

    val npsfiled=psDF.filter(psDF("需求分析是否完成").contains("否")).collect().toList
    var nstepSet:String=func(npsfiled)

    JiraDF.createOrReplaceTempView("otherovermanagesup")

    val midResult=JiraDF.sparkSession.sql(" SELECT dept,team,version,projName,projNo,appShortName,fpd,issueType,issueStatus,entireManageTaskClass,workType,personInCharge,addZero(actualfinishtime) as actualfinishtime,addZero(planFinishDate) AS planFinishDate " +
      " FROM otherovermanagesup" +
      " where entireManageTaskClass !='问题修改' " +
      " AND entireManageTaskClass !='性能测试' " +
      " AND entireManageTaskClass !='专项测试' ")
    personDF.createOrReplaceTempView("person")
    midResult.createOrReplaceTempView("midresult")
    val midOtherOverManageResult=midResult.sparkSession.sql(" SELECT person.user,midresult.dept,midresult.team,midresult.version,midresult.projName,midresult.projNo,midresult.appShortName,midresult.fpd" +
      " FROM midresult,person " +
      " WHERE midresult.issueStatus IN ("+stepSet+") AND midresult.issueType = '整体管理任务' AND midresult.personInCharge !='0' AND midresult.entireManageTaskClass!='项目管理' AND midresult.entireManageTaskClass!='整体设计' AND midresult.entireManageTaskClass!='需求分析' AND midresult.entireManageTaskClass!='自动化测试' AND midresult.entireManageTaskClass!='代码复核' AND midresult.entireManageTaskClass!='配合测试' AND midresult.entireManageTaskClass!='测试设计' AND person.user=midresult.personInCharge AND person.dept=midresult.dept AND person.team=midresult.team AND person.version=midresult.version AND person.projName=midresult.projName AND person.projNo=midresult.projNo AND person.appShortName=midresult.appShortName AND "+acDate +
      " OR (midresult.issueStatus IN ("+nstepSet+") AND midresult.issueType = '整体管理任务' AND midresult.personInCharge !='0' AND midresult.entireManageTaskClass!='项目管理' AND midresult.entireManageTaskClass!='整体设计' AND midresult.entireManageTaskClass!='需求分析' AND midresult.entireManageTaskClass!='自动化测试' AND midresult.entireManageTaskClass!='代码复核' AND midresult.entireManageTaskClass!='配合测试' AND midresult.entireManageTaskClass!='测试设计' AND person.user=midresult.personInCharge AND person.dept=midresult.dept AND person.team=midresult.team AND person.version=midresult.version AND person.projName=midresult.projName AND person.projNo=midresult.projNo AND person.appShortName=midresult.appShortName AND "+plDate +
      " ) ORDER BY dept")

      .groupBy("user","dept","team","version","projName","projNo","appShortName")
      .agg(Map("fpd"->"sum"))
      .withColumnRenamed("sum(fpd)","OtherOverManageWl")

    midOtherOverManageResult.createOrReplaceTempView("prootherovermanage")
    val otherOverManageResult=midOtherOverManageResult.sparkSession.sql(" SELECT * FROM prootherovermanage "+condition)
    otherOverManageResult
  }

  def funcManageSupWl(personDF:DataFrame,JiraDF:DataFrame,psDF:DataFrame,ListDF:DataFrame,PUT:PublicUtilTools,startDate:String,overDate:String,func:List[Row]=>String,condition:String,acDate:String,plDate:String):DataFrame={
    sharetool.timeaddZero(JiraDF);

    val psfiled=psDF.filter(psDF("需求分析是否完成").contains("是")).collect().toList
    var stepSet:String=func(psfiled)

    val npsfiled=psDF.filter(psDF("需求分析是否完成").contains("否")).collect().toList
    var nstepSet:String=func(npsfiled)

    JiraDF.createOrReplaceTempView("managesup")

    val midResult=JiraDF.sparkSession.sql(" SELECT dept,team,version,projName,projNo,appShortName,fpd,issueType,issueStatus,entireManageTaskClass,workType,personInCharge,addZero(actualfinishtime) as actualfinishtime,addZero(planFinishDate) AS planFinishDate FROM managesup")
    personDF.createOrReplaceTempView("person")
    midResult.createOrReplaceTempView("midresult")
    val midOtherManageSupResult=midResult.sparkSession.sql(" SELECT person.user,midresult.dept,midresult.team,midresult.version,midresult.projName,midresult.projNo,midresult.appShortName,midresult.fpd" +
      " FROM midresult,person " +
      " WHERE midresult.issueStatus IN ("+stepSet+") AND midresult.issueType = '管理支持任务' AND midresult.personInCharge !='0'  AND person.user=midresult.personInCharge AND person.dept=midresult.dept AND person.team=midresult.team AND person.version=midresult.version AND person.projName=midresult.projName AND person.projNo=midresult.projNo AND person.appShortName=midresult.appShortName AND "+acDate +
      " OR (midresult.issueStatus IN ("+nstepSet+") AND midresult.issueType = '管理支持任务' AND midresult.personInCharge !='0'  AND person.user=midresult.personInCharge AND person.dept=midresult.dept AND person.team=midresult.team AND person.version=midresult.version AND person.projName=midresult.projName AND person.projNo=midresult.projNo AND person.appShortName=midresult.appShortName AND "+plDate +
      " ) ORDER BY dept")

      .groupBy("user","dept","team","version","projName","projNo","appShortName")
      .agg(Map("fpd"->"sum"))
      .withColumnRenamed("sum(fpd)","ManageSupWl")

    midOtherManageSupResult.createOrReplaceTempView("proothermanagesup")
    val otherManageSupResult=midOtherManageSupResult.sparkSession.sql(" SELECT * FROM proothermanagesup "+condition)
    otherManageSupResult
  }

  def funcResearchWl(personDF:DataFrame,JiraDF:DataFrame,psDF:DataFrame,ListDF:DataFrame,PUT:PublicUtilTools,startDate:String,overDate:String,func:List[Row]=>String,condition:String,acDate:String,plDate:String):DataFrame={
    sharetool.timeaddZero(JiraDF);

    val psfiled=psDF.filter(psDF("需求分析是否完成").contains("是")).collect().toList
    var stepSet:String=func(psfiled)

    val npsfiled=psDF.filter(psDF("需求分析是否完成").contains("否")).collect().toList
    var nstepSet:String=func(npsfiled)

    JiraDF.createOrReplaceTempView("managesup")

    val midResult=JiraDF.sparkSession.sql(" SELECT dept,team,version,projName,projNo,appShortName,fpd,issueType,issueStatus,entireManageTaskClass,workType,personInCharge,addZero(actualfinishtime) as actualfinishtime,addZero(planFinishDate) AS planFinishDate FROM managesup")
    personDF.createOrReplaceTempView("person")
    midResult.createOrReplaceTempView("midresult")
    val midOtherManageSupResult=midResult.sparkSession.sql(" SELECT person.user,midresult.dept,midresult.team,midresult.version,midresult.projName,midresult.projNo,midresult.appShortName,midresult.fpd" +
      " FROM midresult,person " +
      " WHERE midresult.issueStatus IN ("+stepSet+") AND midresult.issueType = '研究任务' AND midresult.personInCharge !='0'  AND person.user=midresult.personInCharge AND person.dept=midresult.dept AND person.team=midresult.team AND person.version=midresult.version AND person.projName=midresult.projName AND person.projNo=midresult.projNo AND person.appShortName=midresult.appShortName AND "+acDate +
      " OR (midresult.issueStatus IN ("+nstepSet+") AND midresult.issueType = '研究任务' AND midresult.personInCharge !='0'  AND person.user=midresult.personInCharge AND person.dept=midresult.dept AND person.team=midresult.team AND person.version=midresult.version AND person.projName=midresult.projName AND person.projNo=midresult.projNo AND person.appShortName=midresult.appShortName AND "+plDate +
      " ) ORDER BY dept")

      .groupBy("user","dept","team","version","projName","projNo","appShortName")
      .agg(Map("fpd"->"sum"))
      .withColumnRenamed("sum(fpd)","ResearchWl")

    midOtherManageSupResult.createOrReplaceTempView("proothermanagesup")
    val otherManageSupResult=midOtherManageSupResult.sparkSession.sql(" SELECT * FROM proothermanagesup "+condition)
    otherManageSupResult
  }
}
