package com.lucius.shu.service.impl

import com.lucius.shu.base.Constants
import com.lucius.shu.dao.impl.BizDao
import com.lucius.shu.service.Service
import com.lucius.shu.util.FileUtils
import org.apache.spark.Logging

object TrendDataService extends Logging {
  def saveFiles = {
    if (Constants.App.SAVE_MIDDLE_FILES) {
      FileUtils.saveAsTextFile(BizDao.getTempSegAndShu, Constants.OutputPath.TEMP_SEG_AND_SHU)
      FileUtils.saveAsTextFile(BizDao.getSegAndShu, Constants.OutputPath.SEG_AND_SHU)
      FileUtils.saveAsTextFile(BizDao.getNoDup, Constants.OutputPath.NO_DUP)
      FileUtils.saveAsTextFile(BizDao.getSegSum, Constants.OutputPath.SEG_SUM)
      FileUtils.saveAsTextFile(BizDao.getDup, Constants.OutputPath.DUP)
      FileUtils.saveAsTextFile(BizDao.getDup2, Constants.OutputPath.DUP2)
      FileUtils.saveAsTextFile(BizDao.getDup3, Constants.OutputPath.DUP3)
      FileUtils.saveAsTextFile(BizDao.getAllData, Constants.OutputPath.ALL_DATA)
      FileUtils.saveAsTextFile(BizDao.getModelData, Constants.OutputPath.MODEL_DATA)
      FileUtils.saveAsTextFile(BizDao.getSeasonIndex, Constants.OutputPath.SEASON_INDEX)
    }

    FileUtils.saveAsTextFile(BizDao.getTrendData, Constants.OutputPath.TREND_DATA)

    if (Constants.App.SAVE_MIDDLE_FILES) {
      FileUtils.saveAsTextFile(BizDao.getTrendForecast, Constants.OutputPath.TREND_FORECAST)
    }

    FileUtils.saveAsTextFile(BizDao.getFinalForecast, Constants.OutputPath.FINAL_FORECAST)
  }
}

class TrendDataService extends Service {
  override protected def runServices() = {
    TrendDataService.saveFiles
  }
}
