package com.learn.log

import com.ggstar.util.ip.IpHelper

/**
  * Created by kimvra on 2019-04-29
  */
object IpUtil extends App {
  def getCity(ip: String) = {
    IpHelper.findRegionByIp(ip)
  }
}
