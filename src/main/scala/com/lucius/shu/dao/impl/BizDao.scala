package com.lucius.shu.dao.impl

import com.lucius.shu.base.Contexts
import com.lucius.shu.dao.SQL
import com.lucius.shu.util.DateUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object BizDao {
  def unionShu() = {
    BaseDao.getShuProps()
      .union(BaseDao.getShuNewProps())
      .map(a => (a(0).toString, a(1).toString, a(2).toString, a(3).toString, a(4).toString, a(5).toString, a(6).toString))
      .distinct()
  }

  /**
   * 获取行业分类
   * 注意:type_id = '47'的是行业分类
   */
  def getSeg = {
    BaseDao.getLinkageProps(SQL().select("id,type_id,name,sign_id").where("type_id = '47'"))
      .map(a => (a(0).toString, a(1).toString, convertSegNameFor(a(2).toString), a(3).toString))
  }

  /**
   * 获取二级行业分类
   * 注意:type_id = '48'的是二级行业分类
   */
  def getSegDetails = {
    BaseDao.getLinkageProps(SQL().select("pid,name,sign_id").where("type_id = '48'"))
      .map(a => (a(0).toString, a(1).toString, a(2).toString))
  }

  /**
   * 获取关键字
   * 注意:type_id = '49'的是关键字
   */
  def getKeyWords = {
    BaseDao.getLinkageProps(SQL().select("id,pid,name,sign_id").where("type_id = '49'")).map(a => (a(0).toString, a(1).toString, a(2).toString, a(3).toString))
  }

  /**
   * 获取关键字的查询数
   * 按关键字和年月分类,取近五年的数据
   * @return
   */
  def getShu = {
    BaseDao.getShuALLProps(SQL().select("type_id,type_name,gmt_target,shu").where(s"gmt_target < '${DateUtils.monthsAgo(0, "yyyy-MM-01 00:00:00")}' and gmt_target >= '${DateUtils.monthsAgo(61, "yyyy-MM-01 00:00:00")}'"))
      .map(a => (a(0).toString, a(1).toString, DateUtils.strToStr(a(2).toString, "yyyy-MM-dd hh:mm:ss", "yyyy-MM"), a(3).toString.toLong))
      .map(t => ((t._1, t._2, t._3), t._4))
      .groupByKey()
      .map(t => (t._1._1, t._1._2, t._1._3, t._2.sum)).persist(StorageLevel.MEMORY_AND_DISK)
  }


  def getTempSegAndShu = {
    getSeg.map(t => (t._4, (t._1, t._2, t._3)))
      .leftOuterJoin(getSegDetails.map(t => (t._1, (t._2, t._3)))) //(12,((1695,47,3C数码),Some((数码相机/单反相机/摄像机,1221))))
      .filter(_._2._2.isDefined)
      .map(t => (t._2._2.get._2, (t._2._1._1, t._2._1._2, t._2._1._3, t._1, t._2._2.get._1))) //(1221,(1695,47,3C数码,12,数码相机/单反相机/摄像机))
      .leftOuterJoin(getKeyWords.map(t => (t._2, (t._1, t._3, t._4)))) //(1221,((1695,47,3C数码,12,数码相机/单反相机/摄像机),Some((2039,单反相机,11221101))))
      .filter(_._2._2.isDefined)
      .map(t => (t._2._2.get._1, (t._2._1._3, t._2._1._5, t._2._2.get._2, t._2._2.get._3)))
      .leftOuterJoin(getShu.map(t => (t._1, (t._2, t._3, t._4)))) //(1845,((服饰,女士内衣/男士内衣/家居服,男士保暖内衣,11012114),Some((男士保暖内衣,2013-05,10989))))
      .filter(_._2._2.isDefined)
      .map(t => (t._2._1._1, t._2._1._2, t._2._2.get._1, t._2._2.get._2, t._2._2.get._3)).persist(StorageLevel.MEMORY_AND_DISK)
  }

  /**
   * 返回：(服饰,连体裤,2011-07,4863271)
   * @return
   */
  def getSegAndShu = {
    getTempSegAndShu.distinct()
      .map(t => ((t._1, t._3, t._4), t._5))
      .groupByKey().map(t => (t._1._1, t._1._2, t._1._3, t._2.max)).persist(StorageLevel.MEMORY_AND_DISK)
  }

  /**
   * 不考虑分类，将同一年月下的关键词出现的次数
   * 返回：(鞋包配饰,高跟单鞋,2015-06,187301,1)
   */
  def getKeywordsNum = {
    val keywordsNum = getSegAndShu.map(t => ((t._2, t._3), 1)).groupByKey().map(t => (t._1, t._2.sum)).persist(StorageLevel.MEMORY_AND_DISK)
    getSegAndShu
      .map(t => ((t._2, t._3), (t._1, t._4)))
      .leftOuterJoin(keywordsNum) //((商务休闲鞋,2011-10),((鞋类箱包,18169),Some(1)))
      .map(t => (t._2._1._1, t._1._1, t._1._2, t._2._1._2, t._2._2.getOrElse(0)))
      .sortBy(t => (t._1, t._2, t._3, t._4)).persist()
  }

  /**
   * 返回：(服装内衣,文胸套装,2012-08,217542)
   */
  def getDup = {
    getKeywordsNum.filter(_._5 > 1).map(t => (t._1, t._2, t._3, t._4)).persist(StorageLevel.MEMORY_AND_DISK)
  }

  /**
   * 对file NoDup，取近五年的数据, 按分类分组后对shu求和），存为SegSum。
   * 返回：(服装内衣,女士棒球帽,2013-08,75393)
   */
  def getNoDup = {
    getKeywordsNum.filter(_._5 == 1).map(t => (t._1, t._2, t._3, t._4)).persist(StorageLevel.MEMORY_AND_DISK)
  }

  /**
   * 返回：(鞋包配饰,2414180935)
   * @return
   */
  def getSegSum = {
    getNoDup.map(t => (t._1, t._4)).groupByKey().map(t => (t._1, t._2.sum))
  }

  /**
   * getShuByNoDupSeg按Seg拼接到getDup上
   * 返回：
   * 母婴用品,帆布鞋,2015-04,2273320,8565377774)
   * (母婴用品,帆布鞋,2015-05,1647389,8565377774)
   * (母婴用品,帆布鞋,2015-06,512664,8565377774)
   * (母婴用品,打底裤,2015-06,1271524,8565377774)
   * (母婴用品,棉衣,2015-06,14283,8565377774)
   * (母婴用品,毛衣,2015-06,1142548,8565377774)
   * (母婴用品,羽绒服,2015-06,54199,8565377774)
   * (母婴用品,衬衫,2015-06,1331481,8565377774)
   */
  def getDup2 = {
    getDup.map(t => (t._1, (t._2, t._3, t._4)))
      .leftOuterJoin(getSegSum)
      .filter(_._2._2.isDefined)
      .map(t => (t._1, t._2._1._1, t._2._1._2, t._2._1._3, t._2._2.get)).persist(StorageLevel.MEMORY_AND_DISK) //(服装内衣,Hodo/红豆,2014-03,133865,5486880354)
  }

  /**
   * 对Dup2，按年月和关键字分组下，对shu_t求和，记为变量shu_tt。计算新变量shu_n = shu * (shu_t / shu_tt)
   */
  def getDup3 = {
    val groupByMonthAndKeyWordRDD = getDup2.map(t => ((t._2, t._3), t._5)).groupByKey().map(t => (t._1, t._2.sum)) //((T恤,2015-06),12479104132)
    getDup2.map(t => ((t._2, t._3), (t._1, t._4, t._5))).leftOuterJoin(groupByMonthAndKeyWordRDD) //1、 ((T恤,2015-02),((母婴用品,900013,8565377774),Some(12199677962)))    2、((T恤,2015-02),((运动户外,900013,3634300188),Some(12199677962)))
      .map(t => (t._2._1._1, t._1._1, t._1._2, t._2._1._2, t._2._1._3, t._2._2.get)) //(家纺居家/家具建材,餐椅,2012-03,180502,1563286725,10128664499)
      .map(t => (t._1, t._2, t._3, t._4, t._5, t._6, ((t._5.toDouble / t._6.toDouble) * t._4.toDouble).toLong)).persist(StorageLevel.MEMORY_AND_DISK)
  }

  def getAllData = {
    getNoDup.union(getDup3.map(t => (t._1, t._2, t._3, t._7))).persist(StorageLevel.MEMORY_AND_DISK)
  }

  /**
   * 返回：(手机数码,2015-05,32003587)
   */
  def getModelData = {
    getAllData.map(t => ((t._1, t._3), t._4)).groupByKey().map(t => (t._1._1, t._1._2, t._2.sum)).persist(StorageLevel.MEMORY_AND_DISK)
  }

  /**
   *
   * 返回：(运动户外,05,82821713,72444321,1.1432464526791548)
   * @return
   */
  def getSeasonIndex = {
    val groupBySegAndMonthRDD = getModelData.map(t => ((t._1, t._2.substring(5, 7)), t._3))
      .groupByKey() //((运动户外,11),CompactBuffer(52917912, 124030548, 85641153, 19586667, 86958726))
      .map(t => (t._1._1, t._1._2, t._2.sum / t._2.size)).persist() //(母婴用品,11(月份),278707587)
    val groupBySegRDD = groupBySegAndMonthRDD.map(t => (t._1, t._3))
        .groupByKey()
        .map(t => (t._1, t._2.sum / t._2.size)) //(母婴用品,171195799)
    groupBySegAndMonthRDD.map(t => (t._1, (t._2, t._3)))
      .leftOuterJoin(groupBySegRDD) //(运动户外,((05,82821713),Some(72444321)))
      .map(t => (t._1, t._2._1._1, t._2._1._2, t._2._2.get, t._2._1._2.toDouble / t._2._2.get.toDouble)).persist(StorageLevel.MEMORY_AND_DISK)
  }

  def getTrendData = {
    getModelData.map(t => ((t._1, t._2.substring(5, 7)), (t._2, t._3)))
      .leftOuterJoin(getSeasonIndex.map(t => ((t._1, t._2), t._5))) //((美食特产,09),((2013-09,5816756),Some(0.9595659959115557)))
      .map(t => (t._1._1, t._2._1._1, t._2._1._2, t._2._2.get, (t._2._1._2 / t._2._2.get).toLong, "actual"))
      .sortBy(t => (t._1, t._2), ascending = false)
  }

  /**
   * 返回：(运动户外,2016-05,28464315,forecast)
   * @return
   */
  def getTrendForecast = {
    val trendDataRDD = BaseDao.getTrendDataProps().map(a => (a(0).toString, a(1).toString, a(4).toString.toLong, a(5).toString)) //家纺居家/家具建材, 2013-06, 29675712, 1.158771800384471, 25609625, actual
    val trendForecastArray = getTrendDataForLastSomeMonths(Integer.MAX_VALUE, trendDataRDD).union(getForecastData(trendDataRDD)).distinct().collect().sortBy(t => (t._1, t._2))
    Contexts.sparkContext.parallelize(trendForecastArray)
  }

  def getFinalForecast = {
    getTrendForecast.map(t => ((t._1, t._2.substring(5, 7)), (t._2, t._3, t._4)))
      .leftOuterJoin(getSeasonIndex.map(t => ((t._1, t._2), t._5))) //((运动户外,01),((2015-01,40848341,actual),Some(0.8284926019252772)))
      .map(t => (t._1._1, t._2._1._1, t._2._1._2, t._2._2.getOrElse(1D), t._2._1._3))
      .map(t => (t._1, t._2, t._3, t._4, (t._3 * t._4).toLong, t._5))
      .sortBy(t => (t._1, t._2))
  }

  private def getForecastData(trendDataRDD: RDD[(String, String, Long, String)]) = {
    var rdd = forecastNextMonth(trendDataRDD)
    (1 to 5).foreach { _ => rdd = forecastNextMonth(rdd) }
    rdd.filter(t => t._4 == "forecast")
  }

  private def forecastNextMonth(trendDataRDD: RDD[(String, String, Long, String)]) = {
    val last12MonthsTrendDataRDD = getTrendDataForLastSomeMonths(12, trendDataRDD)
    val lastMonthsTrendDataRDD = getTrendDataForLastSomeMonths(1, trendDataRDD)
    val forestRDD = lastMonthsTrendDataRDD.map(t => (t._1, DateUtils.monthsAgoForDate(-1, t._2, "yyyy-MM")))
      .leftOuterJoin(last12MonthsTrendDataRDD
      .map(t => (t._1, t._3)).groupByKey()
      .map(t => (t._1, t._2.sum / t._2.size))) //(美食特产,(2015-12,Some(5695889)))
      .map(t => (t._1, t._2._1, t._2._2.get, "forecast"))
    last12MonthsTrendDataRDD.union(forestRDD).sortBy(t => (t._1, t._2), ascending = false)
  }

  private def getTrendDataForLastSomeMonths(num: Int, trendDataRDD: RDD[(String, String, Long, String)]) = {
    val lastSomeMonths = trendDataRDD.map(t => (t._1, t._2)).groupByKey().map(t => (t._1, t._2.toSeq.sorted.reverse.take(num)))
    trendDataRDD.map(t => (t._1, (t._2, t._3, t._4))).leftOuterJoin(lastSomeMonths)
      .map(t => (t._1, t._2._1._1, t._2._1._2, t._2._1._3, t._2._2.get))
      .filter(t => t._5.contains(t._2))
      .map(t => (t._1, t._2, t._3, t._4)) //(家纺居家/家具建材,2015-06,46656771,1.158771800384471,40263985)
  }

  /**
   * 该方法最好从文件中读取(有时间就修改)
   */
  def convertSegNameFor(oldCategoryName: String) = {
    oldCategoryName match {
      case "服饰" => "服装内衣"
      case "鞋类箱包" => "鞋包配饰"
      case "3C数码" => "手机数码"
      case "家装家具家纺" => "家纺居家/家具建材"
      case "家用电器" => "家电办公"
      case "母婴" => "母婴用品"
      case "运动户外" => "运动户外"
      case "食品" => "美食特产"
      case "化妆品(含美容工具)" => "护肤彩妆"
      case "图书音像" => "文化娱乐"
      case "汽车及配件" => "汽车摩托"
      case "居家日用" => "日用百货"
      case "珠宝配饰" => "珠宝手表"
      case "乐器" => "本地生活/虚拟服务/其他"
      case "服务大类" => "本地生活/虚拟服务/其他"
      case _ => oldCategoryName
    }
  }

}
