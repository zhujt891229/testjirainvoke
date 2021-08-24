package icbc.task

import icbc.put.PublicUtilTools
import org.apache.spark.sql.DataFrame

class sumsfpd {
  val put= new PublicUtilTools()
  def resultWorkload(DFlist:List[DataFrame],teamDF:DataFrame,memberInfo:DataFrame):DataFrame={
    var midDF:DataFrame=DFlist(0)
    for(i <- 1 to DFlist.size-1){
      midDF=midDF.join(DFlist(i),Seq("user","dept","team","version","projNo","projName","appShortName"),"leftouter")
    }

    midDF=midDF.join(teamDF,midDF("user").substr(-10,9)===teamDF("LOGIN_NAME"),"leftouter")
      .join(memberInfo,midDF("user").substr(-10,9)===memberInfo("userNo"),"leftouter")

    midDF.createOrReplaceTempView("midDF")
    val midDF2 = put.sparkSession.sql("select * , (case when DEP_NAME is null then dept1 else DEP_NAME end ) as dept_name from midDF")
      .na.fill(0,Array("DesignWl","CodingWl","AutoUnitTestWl","ProModifiWl","CodeRevWl",
      "FuncTestWl","FloTestWl","AutoFuncTestWl","AutoProTestWl","TestDesWl",
      "DemaAnalWl","OverDesWl","ProgramDesWl","OtherOverDesWl","ProManageWl","ManageSupWl","ResearchWl","OtherOverManageWl","CoordTestWl"))

    val midDF3 = midDF2.groupBy("user","dept_name","GROUP_NAME","version","projName","projNo","appShortName","projType")
      .agg(Map("DemaAnalWl"->"sum",
        "DesignWl"->"sum",
        "CodingWl"->"sum",
        "FuncTestWl"->"sum",
        "FloTestWl"->"sum",
        "AutoUnitTestWl"->"sum",
        "AutoFuncTestWl"->"sum",
        "AutoProTestWl"->"sum",
        "CoordTestWl"->"sum",
        "ProModifiWl"->"sum",
        "CodeRevWl"->"sum",
        "OverDesWl"->"sum",
        "TestDesWl"->"sum",
        "ProgramDesWl"->"sum",
        "OtherOverDesWl"->"sum",
        "ProManageWl"->"sum",
        "OtherOverManageWl"->"sum",
        "ManageSupWl"->"sum",
        "ResearchWl"->"sum"))
      .withColumnRenamed("dept_name","dept")
      .withColumnRenamed("GROUP_NAME","team")
      .withColumnRenamed("sum(DemaAnalWl)","DemaAnalWl")
      .withColumnRenamed("sum(DesignWl)","DesignWl")
      .withColumnRenamed("sum(CodingWl)","CodingWl")
      .withColumnRenamed("sum(FuncTestWl)","FuncTestWl")
      .withColumnRenamed("sum(FloTestWl)","FloTestWl")
      .withColumnRenamed("sum(AutoUnitTestWl)","AutoUnitTestWl")
      .withColumnRenamed("sum(AutoFuncTestWl)","AutoFuncTestWl")
      .withColumnRenamed("sum(AutoProTestWl)","AutoProTestWl")
      .withColumnRenamed("sum(CoordTestWl)","CoordTestWl")
      .withColumnRenamed("sum(ProModifiWl)","ProModifiWl")
      .withColumnRenamed("sum(CodeRevWl)","CodeRevWl")
      .withColumnRenamed("sum(OverDesWl)","OverDesWl")
      .withColumnRenamed("sum(TestDesWl)","TestDesWl")
      .withColumnRenamed("sum(ProgramDesWl)","ProgramDesWl")
      .withColumnRenamed("sum(OtherOverDesWl)","OtherOverDesWl")
      .withColumnRenamed("sum(ProManageWl)","ProManageWl")
      .withColumnRenamed("sum(OtherOverManageWl)","OtherOverManageWl")
      .withColumnRenamed("sum(ManageSupWl)","ManageSupWl")
      .withColumnRenamed("sum(ResearchWl)","ResearchWl")

    midDF3
  }

  def devWorkload(midDF:DataFrame):DataFrame={
    val midDFDevTemp=midDF.filter("dept != '杭州产品部'")
    val midDFDev = midDFDevTemp.withColumn("DesignWlBack1",midDFDevTemp("DesignWl"))

    val midDFTestTemp=midDF.filter("dept = '杭州产品部'")
    val midDFTest =midDFTestTemp.withColumn("DesignWlBack1",midDFTestTemp("DesignWl")*0)
    val midDevUnionTest = midDFDev.union(midDFTest)

    val result=midDevUnionTest.withColumn("devWL",midDevUnionTest("DesignWlBack1")
      +midDevUnionTest("CodingWl")
      +midDevUnionTest("AutoUnitTestWl")
      +midDevUnionTest("ProModifiWl")
      +midDevUnionTest("CodeRevWl")
      +midDevUnionTest("OverDesWl")
      +midDevUnionTest("ProgramDesWl")
      +midDevUnionTest("OtherOverDesWl")
    ).drop("DesignWlBack1")
    result
  }

  def testWorkload(midDf:DataFrame):DataFrame={
    val midDFDevTemp = midDf.filter("dept != '杭州产品部'")
    val midDFDev = midDFDevTemp.withColumn("DesignWlBack2",midDFDevTemp("DesignWl")*0)

    val midDFTestTemp = midDf.filter("dept = '杭州产品部'")
    val midDFTest = midDFTestTemp.withColumn("DesignWlBack2",midDFTestTemp("DesignWl"))
    val midDevUnionTest = midDFDev.union(midDFTest)
    val result=midDevUnionTest.withColumn("TestWL",midDevUnionTest("FuncTestWl")
      +midDevUnionTest("FloTestWl")
      +midDevUnionTest("AutoFuncTestWl")
      +midDevUnionTest("AutoProTestWl")
      +midDevUnionTest("TestDesWl")
      +midDevUnionTest("CoordTestWl")
      +midDevUnionTest("DesignWlBack2")
    ).drop("DesignWlBack2")
    result
  }

  def otherWorkload(midDF:DataFrame):DataFrame={
    val result=midDF.withColumn("otherWL",midDF("DemaAnalWl")+midDF("ProManageWl")+midDF("OtherOverManageWl")+midDF("ManageSupWl")+midDF("ResearchWl"))
    result
  }

  def sumWorkload(midDF:DataFrame):DataFrame={
    val result=midDF.withColumn("sumAll",midDF("devWL")+midDF("testWL")+midDF("otherWL")).filter("sumAll > 0")
    result
  }
}
