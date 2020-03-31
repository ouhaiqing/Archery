# -*- coding: utf-8 -*-
""" 
@time: 2020/03/14
"""
import simplejson as json
import requests
from django.contrib.auth.decorators import permission_required

from .instance import instance_resource
from django.http import HttpResponse
from django.conf import settings
from common.utils.extend_json_encoder import ExtendJSONEncoder
from django.http import JsonResponse
from sql.engines.mysql import MysqlEngine
from sql.engines import get_engine
from .models import Instance, IdImportHistory
import uuid
import chardet
import xlrd

__author__ = 'hhyo'

CREATE_TABLE_SQL = "CREATE TABLE `id_import_record` (" +\
          "`bid` varchar(64) NOT NULL COMMENT '批次ID'," +\
          "`oid` varchar(64) NOT NULL COMMENT '单号ID'," +\
          "`create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间'," +\
          "PRIMARY KEY (`bid`, `oid`)" +\
          ") ENGINE=InnoDB DEFAULT CHARSET=utf8;"


INSERT_SQL = "INSERT INTO `id_import_record`(`bid`, `oid`) VALUES"


@permission_required('sql.data_import', raise_exception=True)
def list(request):
    """获取资源组列表"""
    limit = int(request.POST.get('limit'))
    offset = int(request.POST.get('offset'))
    limit = offset + limit

    # 组合筛选项
    filter_dict = dict()
    user = request.user
    if user.is_superuser:
        pass
    # 非管理员，拥有审核权限、资源组粒度执行权限的，可以查看组内所有工单
    else:
        # 先获取用户所在资源组列表
        filter_dict['creator'] = user.username
    search = request.POST.get('search', '')
    if search:
        filter_dict['bid'] = search
    # 过滤搜索条件
    import_obj = IdImportHistory.objects.filter(**filter_dict)
    import_count = import_obj.count()

    import_list = import_obj[offset:limit].values("bid", "file_name", "instance_name", "db_name", "tb_name", "record_count", "type", "is_valid", "creator", "create_time")

    # QuerySet 序列化
    rows = [row for row in import_list]

    result = {"total": import_count, "rows": rows}
    # 返回查询结果
    return HttpResponse(json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
                        content_type='application/json')


@permission_required('sql.data_import', raise_exception=True)
def load(request):
    """
    文件解析
    :param request:
    :return:
    """
    if request.method == "GET":
        result = {"status": 0, "msg": '不支持get上传'}
        res = JsonResponse(result)
    else:
        import_type = request.POST.get('import_type')
        if import_type == '1':
            res = loadid(request)
        else:
            res = loaddata(request)

    return res


#  单号上传
def loadid(request):
    instance_name = request.POST.get('instance_name')
    db_name = request.POST.get('db_name')

    user = request.user
    res = instance_resource(request)
    tabels = json.loads(res.content)
    is_created = False
    result = {}

    if request.method == "GET":
        result = {"status": 0, "msg": '不支持get上传'}
    else:

        try:

            sql_map = {}
            for tb in tabels['data']:
                if tb == 'id_import_record':
                    is_created = True
                    break

            instance = Instance.objects.get(instance_name=instance_name)
            check_engine = get_engine(instance=instance)
            if not is_created:
                # 创建表
                check_engine.execute_batch(db_name=db_name, sqlMap={"sql": CREATE_TABLE_SQL})
            uid = str(uuid.uuid4())
            bid = ''.join(uid.split('-'))
            form_file = request.FILES.get("idfiles")
            names = form_file.name.split('.')
            file_type = names[len(names) - 1]
            if file_type in ['xlsx', 'xls']:   # 支持这两种文件格式
                data = xlrd.open_workbook(filename=None, file_contents=form_file.read())
                tables = data.sheets()  # 得到excel中数据表sheets1，sheets2...
                # 循环获取每个数据表中的数据并写入数据库
                #此处取第一个sheet
                table = tables[0]
                rows = table.nrows  # 总行数
                for row in range(0, rows):  # 从1开始是为了去掉表头
                    row_values = table.row_values(row)  # 每一行的数据
                    if len(row_values) > 0 and row_values[0] != '':
                        sql_map[row_values[0]] = "INSERT INTO `id_import_record`(`bid`, `oid`) VALUES('" + bid + "', " + "'" + str(row_values[0]) + "');"
            else:

                #for b in csvfile.chunks():
                for b in form_file.readlines():
                    coding = chardet.detect(b).get("encoding")
                    if coding is None:
                        coding = "gbk"
                    string = str(b, coding).replace("\r\n", "")
                    if string != '':
                        sval = string.split(",")[0]
                        sql_map[sval] = "INSERT INTO `id_import_record`(`bid`, `oid`) VALUES('" + bid + "', " + "'" + sval + "');"

            r = IdImportHistory.objects.create(
                bid=bid,
                file_name=form_file.name,
                instance_name=request.POST.get("instance_name"),
                db_name=request.POST.get("db_name"),
                tb_name='id_import_record',
                type='1',
                is_valid=2,
                record_count=len(sql_map.keys()),
                creator=user.username
            )
            check_result = check_engine.execute_batch(db_name=db_name, sqlMap=sql_map)
            if check_result == 0:
                result['status'] = 1
                result['msg'] = '单号导入失败'
                r = IdImportHistory.objects.filter(bid=bid).update(
                    is_valid=0,
                )
            else:
                r = IdImportHistory.objects.filter(bid=bid).update(
                    is_valid=1,
                )
                result['status'] = 0
                result['total'] = len(sql_map.keys())
        except Exception as msg:
            r = IdImportHistory.objects.filter(bid=bid).update(
                is_valid=0,
            )
            result['status'] = 1
            result['msg'] = str(msg)

    return JsonResponse(result)


#  数据上传
def loaddata(request):
    instance_name = request.POST.get('instance_name')
    db_name = request.POST.get('db_name')
    tb_name = request.POST.get('tb_name')

    user = request.user
    result = {}
    if tb_name == '':
        result = {"status": 0, "msg": '表名不能为空'}
    else:

        try:
            sql_map = {}
            uid = str(uuid.uuid4())
            bid = ''.join(uid.split('-'))
            form_file = request.FILES.get("datafiles")
            names = form_file.name.split('.')
            file_type = names[len(names) - 1]
            if file_type in ['xlsx', 'xls']:   # 支持这两种文件格式
                data = xlrd.open_workbook(filename=None, file_contents=form_file.read())
                tables = data.sheets()  # 得到excel中数据表sheets1, sheets2...
                # 默认取sheets1
                table = tables[0]
                rows = table.nrows  # 总行数
                for row in range(0, rows):  # 从1开始是为了去掉表头
                    row_values = table.row_values(row)  # 每一行的数据
                    sql = "INSERT INTO `" + tb_name + "` VALUES('" + bid + "'"
                    for r in row_values:
                        sql = sql + ", " + "'" + str(r) + "'"
                    sql = sql +");"
                    sql_map[sql] = sql
            else:

                #for b in csvfile.chunks():
                for b in form_file.readlines():
                    coding = chardet.detect(b).get("encoding")
                    if coding is None:
                        coding = "gbk"
                    string = str(b, coding).replace("\r\n", "")
                    sql = "INSERT INTO `" + tb_name + "` VALUES('" + bid + "'"
                    for r in string.split(","):
                        sql = sql + ", " + "'" + str(r) + "'"
                    sql = sql + ");"
                    sql_map[sql] = sql

            r = IdImportHistory.objects.create(
                bid=bid,
                file_name=form_file.name,
                instance_name=request.POST.get("instance_name"),
                db_name=request.POST.get("db_name"),
                tb_name=tb_name,
                type='2',
                is_valid=2,
                record_count=len(sql_map.keys()),
                creator=user.username
            )
            instance = Instance.objects.get(instance_name=instance_name)
            check_engine = get_engine(instance=instance)
            check_result = check_engine.execute_batch(db_name=db_name, sqlMap=sql_map)
            if check_result == 0:
                result['status'] = 1
                result['msg'] = '单号导入失败'
                r = IdImportHistory.objects.filter(bid=bid).update(
                    is_valid=0,
                )
            else:
                r = IdImportHistory.objects.filter(bid=bid).update(
                    is_valid=1,
                )
                result['status'] = 0
                result['total'] = len(sql_map.keys())
        except Exception as msg:
            r = IdImportHistory.objects.filter(bid=bid).update(
                is_valid=0,
            )
            result['status'] = 1
            result['msg'] = str(msg)

    return JsonResponse(result)