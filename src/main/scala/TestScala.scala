import scala.io.Source

object TestScala extends App {
  override def main(args: Array[String]): Unit = {
    // val field=2
    for (field <- 0 to 3) {
      val deviceList = Source.fromFile("../../Resources/SocialData/DeviceTypeList.csv").getLines().map(x => x.split(",")).map(fields => fields(field).trim()).toList
      val prodDeviceList = Source.fromFile("../../Resources/SocialData/export.csv").getLines().map(x => x.split(",")).map(fields => fields(field).trim()).toList
      //prodDeviceList.foreach(println)
      prodDeviceList.filter(dev => !deviceList.contains(dev)).foreach(println)
      println("-------- END ---------")
      //println(prodDeviceList.contains("sagem VDSL"))
    }
  }
}
