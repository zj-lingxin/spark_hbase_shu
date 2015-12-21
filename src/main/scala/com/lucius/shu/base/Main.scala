package com.lucius.shu.base

import com.lucius.shu.service.impl.{TrendDataService, PrepareService}
import com.lucius.shu.util._
import org.apache.spark.Logging

object Main extends Logging {
  def main(args: Array[String]) {
    val startTime = System.currentTimeMillis()
    if (argsIsIllegal(args)) return
    handleArgs(args)
    runServices()
    closeResources()
    printEndLogs(startTime)
  }

  def handleArgs(args: Array[String])  = {
    Constants.App.TIMESTAMP = args(0).toLong
    //从外部传入的是秒级别的时间戳，所以要乘以1000
    Constants.App.TODAY = DateUtils.timestampToStr(Constants.App.TIMESTAMP * 1000, "yyyyMM/dd")
    if(args.length == 2 && args(1) == "saveMiddleFiles") {
      Constants.App.SAVE_MIDDLE_FILES = true
      logInfo(Utils.logWrapper("""args(1) == "saveMiddleFiles",将产生所有的中间文件"""))
    } else {
      Constants.App.SAVE_MIDDLE_FILES = false
      logInfo(Utils.logWrapper("""args(1) != "saveMiddleFiles",不会产生中间文件"""))
    }
  }

  private def runServices() {
    new PrepareService().run()
    new TrendDataService().run()
  }

  /**
   * 关闭用到的资源
   */
  private def closeResources() = {
    Contexts.stopSparkContext()
  }

  /**
   * 判断传入的参数是否合法
   */
  private def argsIsIllegal(args: Array[String]) = {
    if (Option(args).isEmpty || args.length < 1 || args.length > 2) {
      logError(Utils.logWrapper("请传入程序参数:时间戳、all"))
      true
    } else {
      false
    }
  }

  /**
   * 打印程序运行的时间
   */
  private def printRunningTime(startTime: Long) {
    logInfo(Utils.logWrapper(s"程序共运行${(System.currentTimeMillis() - startTime) / 1000}秒"))
  }

  /**
   * 如果程序在运行过程中出现错误。那么在程序的最后打印出这些错误。
   * 之所以这么做是因为，Spark的Info日志太多，往往会把错误的日志淹没。
   */
  private def printErrorLogsIfExist() {
    if (Constants.App.ERROR_LOG.toString != "") {
      logError(Utils.logWrapper(s"程序在运行过程中遇到了如下错误：${Constants.App.ERROR_LOG.toString}"))
    }
  }

  /**
   * 最后打印出一些提示日志
   */
  private def printEndLogs(startTime: Long): Unit = {
    printErrorLogsIfExist()
    printRunningTime(startTime: Long)
  }

}