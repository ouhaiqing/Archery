# -*- coding: UTF-8 -*-
import logging
import traceback
from itertools import chain

import simplejson as json
from django.contrib.auth.models import Group
from django.http import JsonResponse
from django.db.models import F, Value, IntegerField
from django.http import HttpResponse
from common.utils.extend_json_encoder import ExtendJSONEncoder
from common.utils.permission import superuser_required
from sql.models import ResourceGroup, Instance, DatabaseGroup
from sql.utils.resource_group import user_instances
from django.shortcuts import render

logger = logging.getLogger('default')

#本项目默认跳到view页面的接口都写在views.py里面
@superuser_required
def group(request):
    """资源组管理页面"""
    return render(request, 'databasegroup.html')


@superuser_required
def list(request):
    """获取资源组列表"""
    limit = int(request.POST.get('limit'))
    offset = int(request.POST.get('offset'))
    limit = offset + limit
    search = request.POST.get('search', '')

    # 过滤搜索条件
    group_obj = DatabaseGroup.objects.filter(name__icontains=search)
    group_count = group_obj.count()
    group_list = group_obj[offset:limit].values("id", "name", "instance_name" ,"database_list", "create_time", "update_time")

    # QuerySet 序列化
    rows = [row for row in group_list]

    result = {"total": group_count, "rows": rows}
    # 返回查询结果
    return HttpResponse(json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
                        content_type='application/json')


@superuser_required
def create(request):
    result = {}
    # 返回查询结果
    return HttpResponse(json.dumps(result), content_type='application/json')


@superuser_required
def edit(request):
    id = request.POST.get("id")
    result = DatabaseGroup.objects.get(pk=id)
    # 返回查询结果
    return JsonResponse({'status': 0, 'msg': '', 'data': {"id":result.id,"name":result.name,
                     "instance_name":result.instance_name, "database_list":result.database_list}})


@superuser_required
def save(request):

    id = request.POST.get("id")
    name = request.POST.get("name")
    instance_name = request.POST.get("instance_name")
    database_list = request.POST.get("database_list")
    user = request.user
    filter_result = DatabaseGroup.objects.filter(name=name)

    if id:
        result = DatabaseGroup.objects.filter(id=id).update(
            name=name,
            instance_name=instance_name,
            database_list=database_list,
            creator=user.username,
            modifier=user.username
        )
    else:
        if len(filter_result) > 0:
            return JsonResponse({'status': 500, 'msg': '名称已存在， 操作失败', 'data': []})
        result = DatabaseGroup.objects.create(
            name=name,
            instance_name=instance_name,
            database_list=database_list,
            creator=user.username,
            modifier=user.username
        )


    return JsonResponse({'status': 0, 'msg': '', 'data': []})


@superuser_required
def delete(request):
    delete_id = request.POST.get("id")
    result = DatabaseGroup.objects.filter(id=delete_id).delete()
    # 返回查询结果
    return JsonResponse({'status': 0, 'msg': '', 'data': []})