package com.lucius.shu.base

import com.lucius.shu.util.Utils
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

object Contexts extends Logging {
  private var _sc: SparkContext = _
  private var _hiveContext: HiveContext = _
  private var _sqlContext: SQLContext = _


  def hiveContext: HiveContext = {
    if (_hiveContext == null) {
      logInfo(Utils.logWrapper("对HiveContext进行实例化"))
      _hiveContext = new HiveContext(sparkContext)
    }
    _hiveContext
  }

  def sqlContext: SQLContext = {
    if (_sqlContext == null) {
      logInfo(Utils.logWrapper("对SQLContext进行实例化"))
      _sqlContext = new SQLContext(sparkContext)

    }
    _sqlContext
  }

  def sparkContext: SparkContext = {
    if (_sc == null) {
      logInfo(Utils.logWrapper("对SparkContext进行实例化"))
      _sc = initSparkContext()
    }
    _sc
  }

  def initSparkContext(master: String = null): SparkContext = {
    val conf = new SparkConf().setAppName(Constants.App.NAME)
    val masterInCodes = Option(master)
    val masterInSparkConf = conf.getOption("spark.master")

    (masterInCodes, masterInSparkConf) match {
      case (None, None) =>
        logWarning(Utils.logWrapper(s"集群和程序代码中都没有设置Master参数,在${getClass.getName}的initSparkContext中对它设置成local"))
        conf.setMaster("local")
      case (None, Some(_)) =>
        logInfo(Utils.logWrapper("程序代码中都没有设置Master参数,但是集群中设置了Master参数，使用集群设置的Master参数"))
      case (Some(_), None) =>
        logInfo(Utils.logWrapper("集群中没有设置Master参数，但是程序代码中都设置了Master参数,使用程序代码的Master参数"))
        conf.setMaster(masterInCodes.get)
      case (Some(_), Some(_)) =>
        logInfo(Utils.logWrapper("集群中设置了Master参数，程序代码中也设置了Master参数,程序代码的Master参数覆盖集群传入的Master参数"))
        conf.setMaster(masterInCodes.get)
    }
    logInfo(s"${Constants.App.LOG_WRAPPER} Master = ${conf.get("spark.master")},conf = ${conf.get("spark.app.name")} ${Constants.App.LOG_WRAPPER}")

    this._sc = new SparkContext(conf)
    _sc
  }

  def stopSparkContext() = {
    logInfo(Utils.logWrapper("关闭SparkContext"))
    _sc.stop()
  }
}
