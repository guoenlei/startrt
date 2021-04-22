package com.ald.stat.etl.util

import com.ald.stat.cache.BaseRedisCache
import com.ald.stat.etl.UserGroupETL.{baseRedisKey, markNewUser, otherSceneGroupId, otherSceneId}
import com.ald.stat.etl.model.{JsonRecord, ParquetRecord}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zhaofw on 2018/8/28.
  */
object RecordConverUtil {
  /**
    * 定义StructType
    */
  val structType = StructType(
    Array(
      StructField("ak", StringType, true),
      StructField("uu", StringType, true),
      StructField("att", StringType, true),//将at重命名为att，为日后使用kylin做铺垫
      StructField("pp", StringType, true),
      StructField("ifo", StringType, true),
      StructField("tp", StringType, true),
      StructField("ald_day", StringType, true),//将day重命名为ald_day，为日后使用kylin做铺垫
      StructField("ev", StringType, true),
      StructField("scene", StringType, true),
      StructField("scene_group", StringType, true),
      StructField("qr", StringType, true),
      StructField("qr_group", StringType, true),
      StructField("link_key", StringType, true),
      StructField("media_id", StringType, true),
      StructField("link_scene", StringType, true),
      StructField("province", StringType, true),
      StructField("city", StringType, true),
      StructField("phone_model", StringType, true),
      StructField("phone_brand", StringType, true),
      StructField("wvv", StringType, true),
      StructField("wv", StringType, true),
      StructField("nt", StringType, true),
      StructField("path", StringType, true),
      StructField("ct_key", StringType, true),
      StructField("ct_value", StringType, true)
    )
  )

  /**
    * 把输入的jsonRecord格式转换成用于输出的parquetRecord格式
    * 具体的ETL工作，都在这里完成
    * @param jsonRecord
    * @param parquetRecord
    * @param resource
    * @param sceneMap
    * @param qrMap
    * @param linkMap
    * @param modelMap
    * @param brandMap
    * @return
    */
  def parseLog(jsonRecord:JsonRecord, parquetRecord: ParquetRecord, resource: BaseRedisCache,
               sceneMap: Broadcast[Map[String, String]], qrMap: Broadcast[Map[String, String]],
               linkMap: Broadcast[Map[String, String]], modelMap: Broadcast[Map[String, String]],
               brandMap:Broadcast[Map[String, String]]):ArrayBuffer[Row] ={

    markNewUser(jsonRecord,resource,baseRedisKey)//标记新用户
    parquetRecord.ak = jsonRecord.ak
    parquetRecord.uu = jsonRecord.uu
    parquetRecord.att = jsonRecord.at
    if(jsonRecord.pp != null){
      parquetRecord.pp = jsonRecord.pp
    }else{
      parquetRecord.pp = "null"
    }
    if(jsonRecord.ifo != null){
      parquetRecord.ifo = jsonRecord.ifo
    }else{
      parquetRecord.ifo = "null"
    }
    if(jsonRecord.tp != null){
      parquetRecord.tp = jsonRecord.tp
    }else{
      parquetRecord.tp = "null"
    }
    if(jsonRecord.day != null){
      parquetRecord.ald_day = jsonRecord.day
    }else{
      parquetRecord.ald_day = "null"
    }
    if(jsonRecord.ev != null){
      parquetRecord.ev = jsonRecord.ev
    }else{
      parquetRecord.ev = "null"
    }

    if(jsonRecord.scene != null){
      parquetRecord.scene = jsonRecord.scene
      //val sgid = sceneMap.value.get(jsonRecord.scene.trim)
      if(sceneMap.value.get(jsonRecord.scene.trim) != None){
        val sgid = sceneMap.value.get(jsonRecord.scene.trim).get.toString
        jsonRecord.scene_group_id = sgid
      }else{
        jsonRecord.scene = otherSceneId //其他
        jsonRecord.scene_group_id = otherSceneGroupId
      }

      parquetRecord.scene_group = jsonRecord.scene_group_id
    }else{
      parquetRecord.scene = "null"
      parquetRecord.scene_group = "null"
    }

    if(jsonRecord.qr != null){
      parquetRecord.qr = jsonRecord.qr
      if(qrMap.value.get(jsonRecord.qr.trim) != None){
        val qr_group_key = qrMap.value.get(jsonRecord.qr.trim).get.toString
        jsonRecord.qr_group = qr_group_key
      }else{
        jsonRecord.qr_group = "其它"
      }

      parquetRecord.qr_group = jsonRecord.qr_group
    }else{
      parquetRecord.qr = "null"
      parquetRecord.qr_group = "null"
    }

    if(jsonRecord.wsr_query_ald_link_key != null){
      parquetRecord.link_key = jsonRecord.wsr_query_ald_link_key
      if(linkMap.value.get(jsonRecord.wsr_query_ald_link_key.trim) != None){
        val media_id = linkMap.value.get(jsonRecord.wsr_query_ald_link_key.trim).get.toString
        jsonRecord.wsr_query_ald_media_id = media_id
      }else{
        jsonRecord.wsr_query_ald_media_id = "null"
      }

      if (jsonRecord.scene == "1058" ||
        jsonRecord.scene == "1035" ||
        jsonRecord.scene == "1014" ||
        jsonRecord.scene == "1038") {
        jsonRecord.wsr_query_ald_position_id = jsonRecord.scene
      } else {
        jsonRecord.wsr_query_ald_position_id = "其它"
      }

      parquetRecord.media_id = jsonRecord.wsr_query_ald_media_id
      parquetRecord.link_scene = jsonRecord.wsr_query_ald_position_id
    }else{
      parquetRecord.link_key = "null"
      parquetRecord.media_id = "null"
      parquetRecord.link_scene = "null"
    }

    if(jsonRecord.province != null){
      parquetRecord.province = jsonRecord.province
    } else{
      parquetRecord.province = "null"
    }
    if(jsonRecord.city != null){
      parquetRecord.city = jsonRecord.city
    } else{
      parquetRecord.city = "null"
    }

    if(jsonRecord.pm != null){
      if(brandMap.value.get(jsonRecord.pm.trim) != None){
        val brand = brandMap.value.get(jsonRecord.pm.trim).get.toString
        jsonRecord.brand = brand
      }else{
        jsonRecord.brand = "未知" //如果不存在则把品牌值赋值为"未知"
      }

      if(modelMap.value.get(jsonRecord.pm.trim) != None){
        val model = modelMap.value.get(jsonRecord.pm.trim).get.toString
        jsonRecord.model = model
      }else{
        jsonRecord.model = "未知" //如果不存在则把机型值赋值为"未知"
      }

      parquetRecord.phone_model = jsonRecord.model
      parquetRecord.phone_brand = jsonRecord.brand
    }else{
      parquetRecord.phone_brand = "null"
      parquetRecord.phone_model = "null"
    }

    if(jsonRecord.wvv != null){
      parquetRecord.wvv = jsonRecord.wvv
    }else{
      parquetRecord.wvv = "null"
    }
    if(jsonRecord.wv != null){
      parquetRecord.wv = jsonRecord.wv
    } else{
      parquetRecord.wv = "null"
    }
    if(jsonRecord.nt != null){
      parquetRecord.nt = jsonRecord.nt
    } else{
      parquetRecord.nt = "null"
    }
    if(jsonRecord.path != null){
      parquetRecord.path = jsonRecord.path
    }else{
      parquetRecord.path = "null"
    }

    // TODO: 如果ct字段不为空，则对ct字段进行拆分，返回一个List<Row>
    val record_list = recordToListByCt(parquetRecord)
    val row_list = new ArrayBuffer[Row]()
    record_list.foreach(parquetRecord=>{
      //这个row里面的字段要和structType中的字段对应上
      val row = Row(parquetRecord.ak,parquetRecord.uu,parquetRecord.att,parquetRecord.pp,parquetRecord.ifo,parquetRecord.tp,
        parquetRecord.ald_day,parquetRecord.ev,parquetRecord.scene,parquetRecord.scene_group,parquetRecord.qr,
        parquetRecord.qr_group,parquetRecord.link_key,parquetRecord.media_id,parquetRecord.link_scene,parquetRecord.province,
        parquetRecord.city,parquetRecord.phone_model, parquetRecord.phone_brand, parquetRecord.wvv,parquetRecord.wv,
        parquetRecord.nt,parquetRecord.path,parquetRecord.ct_key,parquetRecord.ct_value)
      row_list += row
    })
  row_list
  }

  /**
    * 把事件参数平铺出来
    * @param parquetRecord
    * @return
    */
  def recordToListByCt(parquetRecord: ParquetRecord):  ArrayBuffer[ParquetRecord] ={
    var record_list = new ArrayBuffer[ParquetRecord]()
    if(StringUtils.isNotBlank(parquetRecord.ct) && parquetRecord.ct.isInstanceOf[String] && parquetRecord.ct.startsWith("{") && parquetRecord.ct.endsWith("}")){
      val json = "{\\\"errMsg\\\":\\\"getShareInfo:ok\\\",\\\"iv\\\":\\\"0kmjMXf1xrhQBAJjThBs9Q==\\\",\\\"encryptedData\\\":\\\"HlM4my00aYLTB2hcw4WbZD20bqshecnETIR1rYpfLrwZJHSFcfS9oGs+GhiwquURc9aQgKqUe7QznGH3JDilGLSEByLUY\\/pLo+qNNr5alDfb3f0D9qZNd\\/dw+vXd7xiD1\\/mjcvDLZVlxP8mp\\/p5t5A==\\\"}"
      val ct = parquetRecord.ct
      val ct_array = ct.split(",")//获取键值对组合
      ct_array.foreach(line=>{
        val key_value_arr = line.split(":")
        if(key_value_arr.length>1){
          val ct_key = key_value_arr(0)
          val ct_value = key_value_arr(1)
          if(ct_key.size>3 && ct_value.size>3){
            var parquet_record:ParquetRecord = parquetRecord
            parquet_record.setCt_key(ct_key.substring(1,ct_key.size-1))
            parquet_record.setCt_value(ct_value.substring(1,ct_value.size-1))
            record_list += parquet_record
          }else{
            parquetRecord.setCt_key("null")
            parquetRecord.setCt_value("null")
            record_list += parquetRecord
          }
        }else{
          parquetRecord.setCt_key("null")
          parquetRecord.setCt_value("null")
          record_list += parquetRecord
        }
      })
    }else{
      parquetRecord.setCt_key("null")
      parquetRecord.setCt_value("null")
      record_list += parquetRecord
    }
    record_list
  }



}
