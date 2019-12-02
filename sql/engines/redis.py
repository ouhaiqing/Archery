# -*- coding: UTF-8 -*-
""" 
@author: hhyo、yyukai
@license: Apache Licence
@file: redis.py
@time: 2019/03/26
"""

import re
import redis
import logging
import traceback
from django.conf import settings

from common.utils.timer import FuncTimer
from . import EngineBase
from .models import ResultSet, ReviewSet, ReviewResult

__author__ = 'hhyo'

logger = logging.getLogger('default')


class RedisEngine(EngineBase):
    def get_connection(self, db_name=None):
        return self.get_redis_connection(self.host, self.port, self.password, db_name)

    @staticmethod
    def get_redis_connection(host, port, password, db_name=None):
        db_name = db_name or 0
        return redis.Redis(host=host, port=port, db=db_name, password=password,
                           encoding_errors='ignore', decode_responses=True)

    @property
    def name(self):
        return 'Redis'

    @property
    def info(self):
        return 'Redis engine'

    def get_all_databases(self):
        """
        获取数据库列表
        :return:
        """
        result = ResultSet(full_sql='CONFIG GET databases')
        conn = self.get_connection()
        rows = conn.config_get('databases')['databases']
        db_list = [str(x) for x in range(int(rows))]
        result.rows = db_list
        return result

    def query_check(self, db_name=None, sql='', limit_num=0):
        """提交查询前的检查"""
        result = {'msg': '', 'bad_query': True, 'filtered_sql': sql, 'has_star': False}
        safe_cmd = ["scan", "exists", "ttl", "pttl", "type", "get", "mget", "strlen",
                    "hgetall", "hexists", "hget", "hmget", "hkeys", "hvals",
                    "smembers", "scard", "sdiff", "sunion", "sismember", "llen", "lrange", "lindex"]
        # 命令校验，仅可以执行safe_cmd内的命令
        for cmd in safe_cmd:
            if re.match(fr'^{cmd}', sql.strip(), re.I):
                result['bad_query'] = False
                break
        if result['bad_query']:
            result['msg'] = "禁止执行该命令！"
        return result

    def query(self, db_name=None, sql='', limit_num=0, close_conn=True):
        """返回 ResultSet """
        result_set = self.execute_query(self.host, self.port, self.password, db_name, sql, limit_num)
        return result_set

    def execute_query(self, host, port, password, db_name, sql, limit_num):
        result_set = ResultSet(full_sql=sql)
        try:
            conn = self.get_redis_connection(host, port, password, db_name=db_name)
            rows = conn.execute_command(sql)
            result_set.column_list = ['Result']
            if isinstance(rows, list):
                result_set.rows = tuple([row] for row in rows)
                result_set.affected_rows = len(rows)
            else:
                result_set.rows = tuple([[rows]])
                result_set.affected_rows = 1 if rows else 0
            if limit_num > 0:
                result_set.rows = result_set.rows[0:limit_num]
        except Exception as e:
            x = str(e)
            list_node = x.split(' ')
            if x.index('MOVED') == 0 and len(list_node) == 3:
                node_info = list_node[2].split(':')
                result_set = self.execute_query(node_info[0], node_info[1], self.password, db_name, sql, limit_num)
            else:
                logger.warning(f"Redis命令执行报错，语句：{sql}， 错误信息：{traceback.format_exc()}")
                result_set.error = str(e)
        return result_set

    def filter_sql(self, sql='', limit_num=0):
        return sql.strip()

    def query_masking(self, db_name=None, sql='', resultset=None):
        """不做脱敏"""
        return resultset

    def execute_check(self, db_name=None, sql=''):

        """提交查询前的检查"""
        safe_cmd = settings.CONFIG_PARAMS['redis']['safe_inception_cmd'];
        # 命令校验，仅可以执行safe_cmd内的命令
        #"""上线单执行前的检查, 返回Review set"""
        check_result = ReviewSet(full_sql=sql)
        split_sql = [cmd.strip() for cmd in sql.split('\n') if cmd.strip()]
        line = 1
        for _sql in split_sql:
            strip = _sql.strip().split(" ")
            check_flag = True
            for cmd in safe_cmd:

                if cmd == strip[0].lower():
                    check_flag = False
                    break
            if  check_flag:
                result = ReviewResult(id=line,
                                      errlevel=2,
                                      stagestatus='驳回不支持语句',
                                      errormessage='禁止执行该命令！',
                                      sql=_sql,
                                      affected_rows=0,
                                      execute_time=0, )
                check_result.error_count += 1;
            else:
                result = ReviewResult(id=line,
                                      errlevel=0,
                                      stagestatus='Audit completed',
                                      errormessage='None',
                                      sql=_sql,
                                      affected_rows=0,
                                      execute_time=0, )
            check_result.rows += [result]
            line += 1
        return check_result

    def execute_workflow(self, workflow):
        sql = workflow.sqlworkflowcontent.sql_content
        execute_result = ReviewSet(full_sql=sql)
        split_sql = [cmd.strip() for cmd in sql.split('\n') if cmd.strip()]
        line = 0
        execute_flag = False
        for cmd in split_sql:
            line += 1
            row_size = len(execute_result.rows)
            # 假设上一条有异常,标记改为true
            if not execute_flag and row_size > 0 and execute_result.rows[row_size - 1].errlevel == 2:
                execute_flag = True
            if execute_flag:
                # 报错语句后面的语句标记为审核通过、未执行，追加到执行结果中
                execute_result.rows.append(ReviewResult(
                    id=line,
                    errlevel=0,
                    stagestatus='Audit completed',
                    errormessage=f'前序语句失败, 未执行',
                    sql=cmd,
                    affected_rows=0,
                    execute_time=0,
                ))
            else:
                execute_result = self.execute_reids_workflow(workflow, cmd, execute_result, self.host, self.port,
                                                             self.password, line)
        return execute_result

    def execute_reids_workflow(self, workflow, sql, execute_result, host, port, password, line):
        """执行上线单，返回Review set"""
        try:
            conn = self.get_redis_connection(host, port, password, db_name=workflow.db_name)
            with FuncTimer() as t:
                conn.execute_command(sql)
            execute_result.rows.append(ReviewResult(
                id=line,
                errlevel=0,
                stagestatus='Execute Successfully',
                errormessage='None',
                sql=sql,
                affected_rows=0,
                execute_time=t.cost,
            ))
        except Exception as e:
            x = str(e)
            list_node = x.split(' ')
            if list_node[0]=='MOVED' and len(list_node) == 3:
                #logger.warning(f"Redis命令执行报错<与该节点不匹配:{x}>，语句：{sql}， 错误信息：{traceback.format_exc()}")
                node_info = list_node[2].split(':')
                execute_result = self.execute_reids_workflow(workflow, sql, execute_result, node_info[0], node_info[1], self.password, line)
            else:
                logger.warning(f"Redis命令执行报错，语句：{sql}， 错误信息：{traceback.format_exc()}")
                # 追加当前报错语句信息到执行结果中
                execute_result.error = str(e)
                execute_result.rows.append(ReviewResult(
                    id=line,
                    errlevel=2,
                    stagestatus='Execute Failed',
                    errormessage=f'异常信息：{e}',
                    sql=sql,
                    affected_rows=0,
                    execute_time=0,
                ))
        return execute_result