package com.lucius.shu.base

object Constants {

  /** App中的常量与每个项目相关 **/
  object App {
    val NAME = "淘宝行业大类的被搜索数的月度预测"
    val LOG_WRAPPER = "##########"
    val YEAR_MONTH_DAY_FORMAT = "yyyy-MM-dd"
    val YEAR_MONTH_FORMAT = "yyyy-MM"
    val DIR = s"${Hadoop.DEFAULT_FS}/shu"
    var TODAY: String = _
    var TIMESTAMP: Long = _
    val ERROR_LOG: StringBuffer = new StringBuffer("")
    var MESSAGES: StringBuffer = new StringBuffer("")
    var SAVE_MIDDLE_FILES = false
  }
  
  object Hadoop {
    val JOBTRACKER_ADDRESS = "appcluster"
    val DEFAULT_FS = s"hdfs://$JOBTRACKER_ADDRESS"
  }
  /** 输入文件路径 **/
  object InputPath {
    val SEPARATOR = "\t"

    private val OFFLINE_DIR = s"${App.DIR}/input/offline/${App.TODAY}/${App.TIMESTAMP}"
    val SYCM_SHU = s"$OFFLINE_DIR/datag_sycm_shu"
    val SYCM_SHU_NEW = s"$OFFLINE_DIR/datag_sycm_shu_new"
    val SYCM_SHU_ALL= s"$OFFLINE_DIR/datag_sycm_shu_all"
    val LINKAGE = s"$OFFLINE_DIR/linkage"
  }
  

  /** 输出文件路径 **/
  object OutputPath {
    val SEPARATOR = "\t"
    private val OFFLINE_DIR = s"${App.DIR}/output/offline/${App.TODAY}/${App.TIMESTAMP}"
    val TEMP_SEG_AND_SHU = s"$OFFLINE_DIR/tempSegAndShu"
    val SEG_AND_SHU = s"$OFFLINE_DIR/segAndShu"
    val SEG_SUM = s"$OFFLINE_DIR/segSum"
    val NO_DUP = s"$OFFLINE_DIR/noDup"
    val DUP = s"$OFFLINE_DIR/dup1"
    val DUP2 = s"$OFFLINE_DIR/dup2"
    val DUP3 = s"$OFFLINE_DIR/dup3"
    val ALL_DATA= s"$OFFLINE_DIR/allData"
    val MODEL_DATA= s"$OFFLINE_DIR/modelData"
    val SEASON_INDEX = s"$OFFLINE_DIR/seasonIndex"
    val TREND_DATA = s"$OFFLINE_DIR/trendData"
    val TREND_FORECAST = s"$OFFLINE_DIR/trendForecast"
    val FINAL_FORECAST = s"$OFFLINE_DIR/finalForecast"

  }

  /** 表的模式 **/
  object Schema {
    //关键字表：id,三级类目的主键ID（linkage表的主键, pid=49),三级类目的名称,抓取目标日期,淘宝指数,创建时间,更新时间
    val SYCM_SHU = "id,type_id,type_name,gmt_target,shu,gmt_created,gmt_modified"

    val LINKAGE = "id,status,sort,type_id,pid,name,value,addtime,addip,sign_id"

    val TREND_DATA = "seg,year_month,shu_seg,ratio_season,trend,flag"
  }
}
