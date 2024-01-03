#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# @author by wangcw 
# @generate at 2024/1/3 10:36

import mysql.connector
from datetime import datetime
import configparser
from loguru import logger
import os
import requests

# 配置、日志设置
config = configparser.ConfigParser()
config.read("db.conf")
logDir = os.path.expanduser("/opt/zrpord/logs")
if not os.path.exists(logDir):
    os.mkdir(logDir)
logFile = os.path.join(logDir, "zrprod.log")
# logger.remove(handler_id=None)

# 配置信息读取
src_host = config.get("prod", "host")
src_port = int(config.get("prod", "port"))
src_database = config.get("prod", "database")
src_user = config.get("prod", "user")
src_password = config.get("prod", "password")

tar_host = config.get("fin", "host")
tar_database = config.get("fin", "database")
tar_user = config.get("fin", "user")
tar_password = config.get("fin", "password")

wx_key = config.get("wx_test", "r_key")
wx_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={}"
wx_headers = {"Content-Type": "application/json"}
wx_mentioned_list = [""]

# SQL

sql_read_1 = "SELECT UUID() AS Id, `MaterialTypeCode`, `MaterialType`, `MainPartId`, `MainPartName`, `TaxAmount`, `BeginDate`, `EndDate`, 0 AS `IsFrozen`, InsertTime, Deleted FROM( SELECT `a`.`TypeCode` AS `MaterialTypeCode`, `a`.`TypeName` AS `MaterialType`, `b`.`MainPartId` AS `MainPartId`, `b`.`MainPartName` AS `MainPartName`, `b`.`Tax` AS `TaxAmount`, `b`.`StartTime` AS `BeginDate`, `b`.`EndTime` AS `EndDate`, `b`.`CreatedAt` AS `InsertTime`, `b`.`Deleted` FROM `tb_materialtype` a JOIN `tb_materialtaxes` b ON b.`MateTypeId` = 'MT0000000001' WHERE a.`TypeCode` LIKE '01%' AND a.`Deleted` = 0 UNION ALL SELECT `a`.`TypeCode` AS `MaterialTypeCode`, `a`.`TypeName` AS `MaterialType`, `b`.`MainPartId` AS `MainPartId`, `b`.`MainPartName` AS `MainPartName`, `b`.`Tax` AS `TaxAmount`, `b`.`StartTime` AS `BeginDate`, `b`.`EndTime` AS `EndDate`, `b`.`CreatedAt` AS `InsertTime`, `b`.`Deleted` FROM `tb_materialtype` a JOIN `tb_materialtaxes` b ON b.`MateTypeId` = 'MT0000000003' WHERE a.`TypeCode` LIKE '03%' AND a.`Deleted` = 0 UNION ALL SELECT `a`.`TypeCode` AS `MaterialTypeCode`, `a`.`TypeName` AS `MaterialType`, `b`.`MainPartId` AS `MainPartId`, `b`.`MainPartName` AS `MainPartName`, `b`.`Tax` AS `TaxAmount`, `b`.`StartTime` AS `BeginDate`, `b`.`EndTime` AS `EndDate`, `b`.`CreatedAt` AS `InsertTime`, `b`.`Deleted` FROM `tb_materialtype` a JOIN `tb_materialtaxes` b ON `b`.`MateTypeId` = 'MT0000000002' WHERE `a`.`TypeCode` LIKE '02%' AND `a`.`Deleted` = 0) aa LIMIT 10;"
sql_read_2 = "select `a`.`Id` AS `Id`,`a`.`MaterialId` AS `MaterialId`,`a`.`MaterialName` AS `MaterialName`,`a`.`FinanceNature` AS `FinanceNature`,`a`.`FinanceNatureName` AS `FinanceNatureName`,`b`.`TypeName` AS `MaterialNatureName`,date_format(`a`.`StartTime`,'%Y-%m-%d') AS `BeginDate`,date_format(`a`.`EndTime`,'%Y-%m-%d') AS `EndDate`,if((`a`.`ServicePeriods` is null),0,`a`.`ServicePeriods`) AS `ServicePeriods`,(case when (`a`.`DefineIncome` = '一次性') then 1 when (`a`.`DefineIncome` = '摊销') then 2 else 0 end) AS `DefineIncome`,`a`.`CreatedAt` AS `InsertTime`,`a`.`Deleted` AS `Deleted` from ((select `view_material_properties`.`Id` AS `Id`,max(`view_material_properties`.`MaterialId`) AS `MaterialId`,max(`view_material_properties`.`MaterialName`) AS `MaterialName`,max(`view_material_properties`.`MaterialTypeCode`) AS `MaterialTypeCode`,max(`view_material_properties`.`StartTime`) AS `StartTime`,max(`view_material_properties`.`EndTime`) AS `EndTime`,max(`view_material_properties`.`CreatedAt`) AS `CreatedAt`,max(`view_material_properties`.`Deleted`) AS `Deleted`,max((case `view_material_properties`.`propParentId` when 'PR0000000105' then `view_material_properties`.`propName` end)) AS `DefineIncome`,max((case `view_material_properties`.`propParentId` when 'PR0000000111' then `view_material_properties`.`propName` end)) AS `ServicePeriods`,max((case `view_material_properties`.`propParentId` when 'PR0000000177' then `view_material_properties`.`propId` end)) AS `FinanceNature`,max((case `view_material_properties`.`propParentId` when 'PR0000000177' then `view_material_properties`.`propName` end)) AS `FinanceNatureName` from (select `a`.`Id` AS `Id`,`a`.`MaterialId` AS `MaterialId`,`a`.`MaterialName` AS `MaterialName`,`a`.`MaterialTypeCode` AS `MaterialTypeCode`,`a`.`StartTime` AS `StartTime`,`a`.`EndTime` AS `EndTime`,`a`.`CreatedAt` AS `CreatedAt`,`t`.`a` AS `propId`,`t`.`b` AS `propName`,`t`.`c` AS `propParentId`,`a`.`Deleted` AS `Deleted` from (`tb_material_version` `a` join json_table(`a`.`PropSet`, '$[*]' columns (`a` varchar(255) character set utf8 path '$.PropId', `b` varchar(255) character set utf8 path '$.PropName', `c` varchar(255) character set utf8 path '$.PropParentId')) `t`)) `view_material_properties` group by `view_material_properties`.`Id` having ((`DefineIncome` is not null) and (`FinanceNature` is not null) and (`FinanceNatureName` is not null))) `a` join `tb_materialtype` `b` on((substr(`a`.`MaterialTypeCode`,1,(char_length(`a`.`MaterialTypeCode`) - 2)) = `b`.`TypeCode`))) where (((`a`.`DefineIncome` = '摊销') and (`a`.`ServicePeriods` is not null) and (`a`.`ServicePeriods` <> 0)) or ((`a`.`DefineIncome` = '一次性') and ((`a`.`ServicePeriods` is null) or (`a`.`ServicePeriods` = 0)))) LIMIT 10;"

sql_tru_1 = "TRUNCATE TABLE revenue_materialtypetaxversion;"
sql_tru_2 = "TRUNCATE TABLE revenue_materialproperties;"

sql_write_1 = "INSERT INTO revenue_materialtypetaxversion_py(Id,MaterialTypeCode,MaterialType,MainPartId,MainPartName,TaxAmount,BeginDate,EndDate,IsFrozen,InsertTime,Deleted) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
sql_write_2 = "INSERT INTO revenue_materialproperties_py(Id,MaterialId,MaterialName,FinanceNature,FinanceNatureName,MaterialNatureName,BeginDate,EndDate,ServicePeriods,DefineIncome,InsertTime,Deleted) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"


def send_msg(content_msg, in_wx_key):
    data = {
        "msgtype": "text",
        "text": {"content": content_msg, "mentioned_list": wx_mentioned_list},
    }
    r = requests.post(url=wx_url.format(in_wx_key), json=data, headers=wx_headers)
    return r


# 连接建立
src_con = mysql.connector.connect(
    host=src_host,
    port=src_port,
    user=src_user,
    password=src_password,
    database=src_database,
    buffered=True,
)

tar_con = mysql.connector.connect(
    host=tar_host,
    user=tar_user,
    password=tar_password,
    database=tar_database,
    buffered=True,
)

# 开始时间
start_time = datetime.now()

try:

    # 读取数据
    src_cur = src_con.cursor(dictionary=True)

    src_cur.execute(sql_read_1)
    src_res_1 = src_cur.fetchall()

    src_cur.execute(sql_read_2)
    src_res_2 = src_cur.fetchall()

    src_cur.close()

    # 处理数据

    # 写入数据
    tar_cur = tar_con.cursor(dictionary=True)

    tar_cur.execute(sql_tru_1)
    tar_cur.execute(sql_tru_2)

    tar_cur.execute(sql_write_1, [src_res_1])
    af_rows_1 = tar_cur.rowcount

    tar_cur.execute(sql_write_2, [src_res_2])
    af_rows_2 = tar_cur.rowcount

    tar_cur.close()
    tar_con.commit()

    # 发送企业微信消息
    end_time = datetime.now()
    sp_second = (end_time - start_time).total_seconds()
    logger.info(f"全部数据迁移完成，累计耗时 {sp_second} 秒！")

    wx_message = f'本月数据迁移完毕，其中物料基础属性{af_rows_2}条，物料类型税率版本{af_rows_1}条，耗时{sp_second}秒。'
    send_msg(wx_message, wx_key)

except Exception as e:
    tar_con.rollback()
    logger.exception('数据写入错误！原因：', e)

finally:
    src_con.close()
    tar_con.close()
