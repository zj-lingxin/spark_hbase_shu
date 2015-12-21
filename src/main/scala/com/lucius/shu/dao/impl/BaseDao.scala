package com.lucius.shu.dao.impl

import com.lucius.shu.base._
import com.lucius.shu.dao.{Dao, SQL}

object BaseDao extends Dao {
  def getShuProps(sql: SQL = new SQL()) = getProps(Constants.InputPath.SYCM_SHU, Constants.Schema.SYCM_SHU, "sycm_shu", sql)
  def getShuNewProps(sql: SQL = new SQL()) = getProps(Constants.InputPath.SYCM_SHU_NEW, Constants.Schema.SYCM_SHU, "sycm_shu_new", sql)
  def getShuALLProps(sql: SQL = new SQL()) = getProps(Constants.InputPath.SYCM_SHU_ALL, Constants.Schema.SYCM_SHU, "sycm_shu_all", sql)
  def getLinkageProps(sql: SQL = new SQL()) = getProps(Constants.InputPath.LINKAGE, Constants.Schema.LINKAGE, "linkage", sql)
  def getTrendDataProps(sql: SQL = new SQL()) = getProps(Constants.OutputPath.TREND_DATA, Constants.Schema.TREND_DATA, "trend_data", sql)
}
