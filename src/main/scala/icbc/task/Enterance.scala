package icbc.task

object Enterance {
  val addressHDFSOutputMonth = "D:/jiraOutPut/month"
  val addressHDFSoutputWeek = "D:/jiraOutPut/week"

  def main(args:Array[String]):Unit={
    val newWorkload = new NewWorkload

//    val source=Source.fromFile("/data/hzyfzc/hzyfzcdata/prog_ws/workload/workload.properties","UTF-8")
//    val prop = new Properties()
//    prop.load(source.bufferedReader())
//    val startDateMonth:String = prop.getProperty("startDateOfMonth")
//    val endDateMonth:String = prop.getProperty("endDateOfMonth")
//    val startDateWeek:String = prop.getProperty("startDateOfWeek")
//    val endDateWeek:String = prop.getProperty("endDateOfWeek")

    val startDateMonth:String = "2021/06/26"
    val endDateMonth:String = "2021/07/25"
    val startDateWeek:String = "2021/07/02"
    val endDateWeek:String = "2021/07/08"


    newWorkload.funcActualPlanPeriod(startDateMonth,endDateMonth,addressHDFSOutputMonth)

//    newWorkload.funcActualPlanPeriod(startDateWeek,endDateWeek,addressHDFSoutputWeek)
  }
}
