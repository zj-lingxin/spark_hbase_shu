package com.lucius.shu.service.impl

import com.lucius.shu.base.Constants
import com.lucius.shu.dao.impl.BizDao
import com.lucius.shu.service.Service
import com.lucius.shu.util.FileUtils

class PrepareService extends Service {
  override protected def runServices(): Unit = {
    FileUtils.saveAsTextFile(BizDao.unionShu(),Constants.InputPath.SYCM_SHU_ALL)
  }
}
