package icbc.task

import icbc.put.PublicUtilTools
import icbc.shareTools.shareToolForUDF
import org.apache.spark.sql.DataFrame

class NewWorkload {
//  val addressJira=      "hdfs://hybigdata001:8020/user/hadoop/hzyfzc/midResult/jira/jira.csv"
//  val addressParamState="hdfs://hybigdata001:8020/user/hadoop/hzyfzc/midResult/jira/jira.csv"
//  val projectList=      "hdfs://hybigdata001:8020/user/hadoop/hzyfzc/midResult/jira/jira.csv"
//  val addressMemberInfo="hdfs://hybigdata001:8020/user/hadoop/hzyfzc/midResult/jira/jira.csv"
//  val addressSpical=    "hdfs://hybigdata001:8020/user/hadoop/hzyfzc/midResult/jira/jira.csv"

  val addressJira=      "D:/jiraFiles/jira.csv"
  val addressParamState="D:/jiraFiles/jiracompleteinfo.csv"
  val projectList=      "D:/jiraFiles/importantprojectlist.csv"
  val addressMemberInfo="D:/jiraFiles/memberInfo.csv"
  val addressSpical=    "D:/jiraFiles/specialemployee.csv"


  val publicUtilTools = new PublicUtilTools

  val memberInfo = publicUtilTools.load(addressMemberInfo).cache()
    .filter("memberStatus = '离职'")
    .select("userNo","dept")
    .withColumnRenamed("dept","dept1")

  val teamDF = publicUtilTools.load(addressSpical).cache()
    .filter("version like '2020年9月份版本%'")
    .withColumnRenamed("userNo","LOGIN_NAME")
    .withColumnRenamed("dutyDept","DEP_NAME")
    .withColumnRenamed("team","GROUP_NAME")
    .select("LOGIN_NAME","DEP_NAME","GROUP_NAME")

//  val psDF =publicUtilTools.load(addressParamState).cache()
//  val ListDF=publicUtilTools.load(projectList)

//  val testDF=publicUtilTools.load(addressJira).cache()
  val JiraDF=publicUtilTools.load(addressJira).cache().na.fill("[]",Array("handleUser","designer","programer","tester","flowTester","personInCharge","flowTestDesigner","dept","team","version","projName","projNo","appShortName"))

  val psDF =publicUtilTools.load(addressParamState).cache()
  val ListDF=publicUtilTools.load(projectList)

  val sumsfpd=new sumsfpd
  val personWorkload = new WorkloadIndex()

  val WorkloadIndex=personWorkload.functionExtractPerson(JiraDF,publicUtilTools)
  val DateWorkload = new TimePeriod()
  val transString = new shareToolForUDF()

  def funcActualPlanPeriod(startDate:String,overDate:String,outputPath:String):Unit={
    publicUtilTools.info("startDate="+startDate+",overDate="+overDate)

    val plDate:String=" midresult.planFinishDate IS NOT NULL AND midresult.planFinishDate>='"+startDate+"' AND midresult.planFinishDate<='"+overDate+"' "
    val acDate:String=" ((midresult.actualfinishtime IS NOT NULL AND midresult.actualfinishtime>= '"+startDate+"' and midresult.actualfinishtime<='"+overDate+"' ) OR ( midresult.actualfinishtime IS NULL AND midresult.planFinishDate>= '"+startDate+"' AND midresult.planFinishDate<='"+overDate+"')) "

    val demandResulta=DateWorkload.funcDemaAnalWl(WorkloadIndex,JiraDF,psDF,ListDF,publicUtilTools,startDate,overDate,transString.listToSetStrings,transString.proselect(ListDF,transString.listToSetString)(2))
    val designResulta=DateWorkload.funcDesignWl(WorkloadIndex,JiraDF,psDF,ListDF,publicUtilTools,startDate,overDate,transString.listToSetStrings,transString.proselect(ListDF,transString.listToSetString)(2))
    val codeResulta=DateWorkload.funcCodingWl(WorkloadIndex,JiraDF,psDF,ListDF,publicUtilTools,startDate,overDate,transString.listToSetStrings,transString.proselect(ListDF,transString.listToSetString)(2))
    val funcTestResulta=DateWorkload.funcFuncTestWl(WorkloadIndex,JiraDF,psDF,ListDF,publicUtilTools,startDate,overDate,transString.listToSetStrings,transString.proselect(ListDF,transString.listToSetString)(2))
    val floTestResulta=DateWorkload.funcFloTestWl(WorkloadIndex,JiraDF,psDF,ListDF,publicUtilTools,startDate,overDate,transString.listToSetStrings,transString.proselect(ListDF,transString.listToSetString)(2))
    val autoUnitTestResulta=DateWorkload.funcAutoUnitTestWl(WorkloadIndex,JiraDF,psDF,ListDF,publicUtilTools,startDate,overDate,transString.listToSetStrings,transString.proselect(ListDF,transString.listToSetString)(2),acDate,plDate)
    val autoFuncTestResulta=DateWorkload.funcAutoFuncTestWl(WorkloadIndex,JiraDF,psDF,ListDF,publicUtilTools,startDate,overDate,transString.listToSetStrings,transString.proselect(ListDF,transString.listToSetString)(2),acDate,plDate)
    val autoProTestResulta=DateWorkload.funcAutoProTestWl(WorkloadIndex,JiraDF,psDF,ListDF,publicUtilTools,startDate,overDate,transString.listToSetStrings,transString.proselect(ListDF,transString.listToSetString)(2),acDate,plDate)
    val proMidifiResulta=DateWorkload.funcProModifiWl(WorkloadIndex,JiraDF,psDF,ListDF,publicUtilTools,startDate,overDate,transString.listToSetStrings,transString.proselect(ListDF,transString.listToSetString)(2),acDate,plDate)
    val codRevResulta=DateWorkload.funcCodeRevWl(WorkloadIndex,JiraDF,psDF,ListDF,publicUtilTools,startDate,overDate,transString.listToSetStrings,transString.proselect(ListDF,transString.listToSetString)(2),acDate,plDate)
    val overDesResulta=DateWorkload.funcOverDesWl(WorkloadIndex,JiraDF,psDF,ListDF,publicUtilTools,startDate,overDate,transString.listToSetStrings,transString.proselect(ListDF,transString.listToSetString)(2),acDate,plDate)
    val testDesResulta=DateWorkload.funcTestDesWl(WorkloadIndex,JiraDF,psDF,ListDF,publicUtilTools,startDate,overDate,transString.listToSetStrings,transString.proselect(ListDF,transString.listToSetString)(2),acDate,plDate)
    val programDesResulta=DateWorkload.funcProgramDesWl(WorkloadIndex,JiraDF,psDF,ListDF,publicUtilTools,startDate,overDate,transString.listToSetStrings,transString.proselect(ListDF,transString.listToSetString)(2),acDate,plDate)
    val otherOverDesResulta=DateWorkload.funcOtherOverDesWl(WorkloadIndex,JiraDF,psDF,ListDF,publicUtilTools,startDate,overDate,transString.listToSetStrings,transString.proselect(ListDF,transString.listToSetString)(2),acDate,plDate)
    val coordTestResulta=DateWorkload.funcCoordTestWl(WorkloadIndex,JiraDF,psDF,ListDF,publicUtilTools,startDate,overDate,transString.listToSetStrings,transString.proselect(ListDF,transString.listToSetString)(2),acDate,plDate)
    val proManageResulta=DateWorkload.funcProManageWl(WorkloadIndex,JiraDF,psDF,ListDF,publicUtilTools,startDate,overDate,transString.listToSetStrings,transString.proselect(ListDF,transString.listToSetString)(2),acDate,plDate)
    val otherOverManageResulta=DateWorkload.funcOtherOverManageWl(WorkloadIndex,JiraDF,psDF,ListDF,publicUtilTools,startDate,overDate,transString.listToSetStrings,transString.proselect(ListDF,transString.listToSetString)(2),acDate,plDate)
    val manageSupResulta=DateWorkload.funcManageSupWl(WorkloadIndex,JiraDF,psDF,ListDF,publicUtilTools,startDate,overDate,transString.listToSetStrings,transString.proselect(ListDF,transString.listToSetString)(2),acDate,plDate)
    val researchResultV=DateWorkload.funcResearchWl(WorkloadIndex,JiraDF,psDF,ListDF,publicUtilTools,startDate,overDate,transString.listToSetStrings,transString.proselect(ListDF,transString.listToSetString)(2),acDate,plDate)

    val DFlista:List[DataFrame] = WorkloadIndex::demandResulta::designResulta::codeResulta::funcTestResulta::
    floTestResulta::autoUnitTestResulta::autoFuncTestResulta::autoProTestResulta::coordTestResulta::proMidifiResulta::codRevResulta::overDesResulta::
    testDesResulta::programDesResulta::otherOverDesResulta::proManageResulta::otherOverManageResulta::manageSupResulta::researchResultV::Nil

    val midDFa = sumsfpd.resultWorkload(DFlista,teamDF,memberInfo)
    val midDFWithDeva = sumsfpd.devWorkload(midDFa)
    val midDFWithDevAndTesta=sumsfpd.testWorkload(midDFWithDeva)
    val midDFWithDevTestOtera=sumsfpd.otherWorkload(midDFWithDevAndTesta)
    val finalResulta=sumsfpd.sumWorkload(midDFWithDevTestOtera)

    finalResulta.createOrReplaceTempView("finalResult")
    val finalResultExa=finalResulta.sparkSession.sql("SELECT * FROM finalResult "+ transString.proselect(ListDF,transString.listToSetString)(2))

    val finalOutResulta = finalResultExa.withColumnRenamed("user","姓名")
      .withColumnRenamed("dept","所属部门")
      .withColumnRenamed("team","所属团队")
      .withColumnRenamed("version","所属版本")
      .withColumnRenamed("projName","项目名称")
      .withColumnRenamed("projNo","项目编号")
      .withColumnRenamed("appShortName","所属应用")
      .withColumnRenamed("projType","项目类型")
      .withColumnRenamed("DemaAnalWl","需求分析工作量")
      .withColumnRenamed("DesignWl","详细设计工作量")
      .withColumnRenamed("CodingWl","编码工作量")
      .withColumnRenamed("FuncTestWl","功能测试工作量")
      .withColumnRenamed("FloTestWl","流程测试工作量")
      .withColumnRenamed("AutoUnitTestWl","自动化单元测试工作量")
      .withColumnRenamed("AutoFuncTestWl","自动化功能测试工作量")
      .withColumnRenamed("AutoProTestWl","自动化流程测试工作量")
      .withColumnRenamed("ProModifiWl","问题修改工作量")
      .withColumnRenamed("CodeRevWl","代码复核工作量")
      .withColumnRenamed("OverDesWl","总体设计工作量")
      .withColumnRenamed("TestDesWl","测试设计工作量")
      .withColumnRenamed("ProgramDesWl","程序设计工作量")
      .withColumnRenamed("CoordTestWl","配合测试工作量")
      .withColumnRenamed("OtherOverDesWl","其他整体设计工作量")
      .withColumnRenamed("ProManageWl","项目管理工作量")
      .withColumnRenamed("OtherOverManageWl","其他整体管理工作量")
      .withColumnRenamed("ManageSupWl","管理支持工作量")
      .withColumnRenamed("ResearchWl","研究任务工作量")
      .withColumnRenamed("devWL","开发工作量")
      .withColumnRenamed("testWL","测试工作量")
      .withColumnRenamed("otherWL","其他工作量")
      .withColumnRenamed("sumAll","总工作量")

    val finalOutResultaTemp=finalOutResulta.withColumn("开始时间"+startDate,finalOutResulta("所属部门")*0).withColumn("结束时间"+overDate,finalOutResulta("所属部门")*0)
    //publicUtilTools.saveHdfs(finalOutResultaTemp,outputPath,"AutualPlanAllProjectWorkload")
    finalOutResultaTemp.repartition(1).write.option("header",true).csv("D:\\jiraOutPut\\month\\AllProjectWorkloadzctualPA.csv")
  }


}
