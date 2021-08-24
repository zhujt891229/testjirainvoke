package icbc.task

import icbc.put.PublicUtilTools
import org.apache.spark.sql.DataFrame

class WorkloadIndex {
  def functionExtractPerson(JiraDF:DataFrame,PUT:PublicUtilTools):DataFrame={
    val noNullJiraDF=JiraDF.na.fill("[]",Array("handleUser","designer","programer","tester","flowTester","personInCharge","flowTestDesigner","dept","team","version","projName","projNo","appShortName"))
    noNullJiraDF.createOrReplaceTempView("noNullJiraDF")

    val result=noNullJiraDF.sparkSession.sql(" SELECT handleUser AS user,dept,team,version,projName,projNo,appShortName" +
      " FROM noNullJiraDF" +
      " UNION " +
      " SELECT designer AS user,dept,team,version,projName,projNo,appShortName" +
      " FROM noNullJiraDF " +
      " UNION " +
      " SELECT programer AS user,dept,team,version,projName,projNo,appShortName" +
      " FROM noNullJiraDF " +
      " UNION " +
      " SELECT tester AS user,dept,team,version,projName,projNo,appShortName" +
      " FROM noNullJiraDF " +
      " UNION " +
      " SELECT flowTester AS user,dept,team,version,projName,projNo,appShortName" +
      " FROM noNullJiraDF " +
      " UNION " +
      " SELECT personInCharge AS user,dept,team,version,projName,projNo,appShortName" +
      " FROM noNullJiraDF " +
      " UNION " +
      " SELECT flowTestDesigner AS user,dept,team,version,projName,projNo,appShortName" +
      " FROM noNullJiraDF ")

    val projectList="D:/jiraFiles/importantprojectlist.csv"
    val ListDF=PUT.load(projectList)

    val finalRes=result.join(ListDF.drop("projName"),Seq("projNo"),"left_outer").select("user","dept","team","version","projName","projNo","appShortName","projType").distinct()
    finalRes
  }
}
