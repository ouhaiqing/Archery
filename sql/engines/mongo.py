# -*- coding: UTF-8 -*-
import re
import pymongo
import logging
import traceback
import json

from . import EngineBase
from .models import ResultSet, ReviewResult, ReviewSet
from bson import json_util
from common.utils.timer import FuncTimer
from pymongo import operations

__author__ = 'jackie'

logger = logging.getLogger('default')


class MongoEngine(EngineBase):
    def get_connection(self, db_name=None):
        conn = pymongo.MongoClient(self.host, self.port, connect=True, connectTimeoutMS=10000)
        if self.user and self.password:
            if self.db_name:
                conn[self.db_name].authenticate(self.user, self.password)
            else:
                conn.admin.authenticate(self.user, self.password)
        return conn

    @property
    def name(self):  # pragma: no cover
        return 'Mongo'

    @property
    def info(self):  # pragma: no cover
        return 'Mongo engine'

    def get_all_databases(self):
        result = ResultSet(full_sql='get databases')
        conn = self.get_connection()
        if self.db_name:
            result.rows = [self.db_name]
        else:
            result.rows = conn.list_database_names()
        return result

    def query_check(self, db_name=None, sql=''):
        """提交查询前的检查"""
        result = {'msg': '', 'bad_query': True, 'filtered_sql': sql, 'has_star': False}
        safe_cmd = ['find', 'count', 'aggregate']
        start = sql.find("(")
        ope_expression_pre = sql[0:start]
        ope_expression_split = ope_expression_pre.split('.')

        ope = ope_expression_split[len(ope_expression_split)-1]
        for cmd in safe_cmd:
            if cmd == ope:
                result['bad_query'] = False
                break
        if result['bad_query']:
            result[
                'msg'] = """禁止执行该命令！目前查找命令只支持find，count，aggregate！正确格式为：<b>db.collection_name.find[count]({expression})  或者  db.collection_name.aggregate(<font color='red'>[</font>expression<font color='red'>]</font>)</b>"""
        return result

    def get_all_tables(self, db_name):
        result = ResultSet(full_sql='get tables')
        conn = self.get_connection()
        db = conn[db_name]
        result.rows = db.list_collection_names()
        return result

    def query(self, db_name=None, sql='', limit_num=0, close_conn=True):
        result_set = ResultSet(full_sql=sql)
        try:
            conn = self.get_connection()
            db = conn[db_name]

            start = sql.find("(")
            ope_expression_pre = sql[0:start]
            ope_expression_split = ope_expression_pre.split('.')
            ope = ope_expression_split[len(ope_expression_split) - 1]
            end = len(ope)
            collect = db[ope_expression_pre[3:len(ope_expression_pre)-end-1]]

            match = re.compile(r'[(](.*)[)]', re.S)
            sql = re.findall(match, sql)[0].replace("\'", "\"")

            # 普通查询是json，一般查询是数组
            if ope == 'find':
                if sql != '':
                    sql = json.loads(sql)
                    result = collect.find(sql).limit(limit_num)
                else:
                    result = collect.find({}).limit(limit_num)
            elif ope == 'count':
                if sql != '':
                    sql = json.loads(sql)
                    result = collect.count(filter=sql)
                else:
                    result = collect.count(filter={})
                result_set.affected_rows = 1
                result_set.rows = [[result]]
            elif ope == 'aggregate':
                if sql != '':
                    sql = json.loads(sql)
                    result = collect.aggregate(pipeline=sql)
                else:
                    # 应该需要设置 limit
                    result = collect.aggregate(pipeline=[])
            rows = json.loads(json_util.dumps(result))
            result_set.column_list = ['Result']
            if isinstance(rows, list):
                result_set.rows = tuple([json.dumps(x, ensure_ascii=False)] for x in rows)
                result_set.affected_rows = len(rows)
        except Exception as e:
            logger.warning(f"Mongo命令执行报错，语句：{sql}， 错误信息：{traceback.format_exc()}")
            result_set.error = str(e)
        return result_set

    def filter_sql(self, sql='', limit_num=0):
        return sql.strip()

    def query_masking(self, db_name=None, sql='', resultset=None):
        """不做脱敏"""
        return resultset

    def execute_check(self, db_name=None, sql=''):
        """上线单执行前的检查, 返回Review set"""
        # safe_cmd = ['createCollection', 'createIndex', 'insert', 'insertMany', 'update','updateMany','delete','deleteMany', 'remove']
        safe_cmd = ['createCollection', 'createIndex', 'insert', 'insertMany', 'update', 'updateMany', 'remove']
        check_result = ReviewSet(full_sql=sql)
        # 根据分号分割
        split_sql = [cmd.strip() for cmd in sql.split(';') if cmd.strip()]
        line = 1
        for _sql in split_sql:
            start = _sql.find("(")
            ope_expression_pre = _sql[0:start]
            ope_expression_split = ope_expression_pre.split('.')
            ope = ope_expression_split[len(ope_expression_split) - 1]

            check_flag = True
            for cmd in safe_cmd:
                if cmd == ope:
                    check_flag = False
                    break
            if check_flag:
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
        """执行上线单，返回Review set"""
        sql = workflow.sqlworkflowcontent.sql_content
        split_sql = [cmd.strip() for cmd in sql.split(';') if cmd.strip()]
        execute_result = ReviewSet(full_sql=sql)
        line = 1
        _sql = None
        try:
            conn = self.get_connection()
            db = conn[workflow.db_name]
            for _sql in split_sql:
                with FuncTimer() as t:
                    rows_affect = self._parse_execute(db=db, sql=_sql)
                execute_result.rows.append(ReviewResult(
                    id=line,
                    errlevel=0,
                    stagestatus='Execute Successfully',
                    errormessage='None',
                    sql=_sql,
                    affected_rows=rows_affect,
                    execute_time=t.cost,
                ))
                line += 1
        except Exception as e:
            logger.warning(f"Redis命令执行报错，语句：{_sql}， 错误信息：{traceback.format_exc()}")
            # 追加当前报错语句信息到执行结果中
            execute_result.error = str(e)
            execute_result.rows.append(ReviewResult(
                id=line,
                errlevel=2,
                stagestatus='Execute Failed',
                errormessage=f'异常信息：{e}',
                sql=_sql,
                affected_rows=0,
                execute_time=0,
            ))
            line += 1
            # 报错语句后面的语句标记为审核通过、未执行，追加到执行结果中
            for statement in split_sql[line - 1:]:
                execute_result.rows.append(ReviewResult(
                    id=line,
                    errlevel=0,
                    stagestatus='Audit completed',
                    errormessage=f'前序语句失败, 未执行',
                    sql=statement,
                    affected_rows=0,
                    execute_time=0,
                ))
                line += 1
        return execute_result

    @staticmethod
    def _parse_execute(db, sql):
        start = sql.find("(")
        ope_expression_pre = sql[0:start]
        ope_expression_split = ope_expression_pre.split('.')
        ope = ope_expression_split[len(ope_expression_split) - 1]
        end = len(ope)
        collect = db[ope_expression_pre[3:len(ope_expression_pre) - end - 1]]

        match = re.compile(r'[(](.*)[)]', re.S)
        sql = re.findall(match, sql)[0].replace("\'", "\"")
        rows_affect = 0
        if ope == 'createCollection':
            result = db.create_collection(name=sql)
        elif ope == 'createIndex':
            result = collect.create_index(keys=sql)
        # 由于sql的参数解析细粒度比较大
        elif ope == 'insert':
            ids = collect.insert(doc_or_docs=json.loads(sql))
            rows_affect = 1
        elif ope == 'insertMany':
            ids = collect.insert_many(documents=json.loads(sql))
            rows_affect = len(ids.inserted_ids)
        elif ope == 'update':
            jsql = '[' + sql + ']'
            parm_list = json.loads(jsql)
            multi = False
            if len(parm_list)>2 and parm_list[2]['multi']!=None:
                multi = parm_list[2]['multi']
            result = collect.update(spec=parm_list[0], document=parm_list[1], upsert=False, manipulate=False,
                                    multi=multi, check_keys=True)
            rows_affect = result['nModified']
        elif ope == 'updateMany':
            jsql = '[' + sql + ']'
            parm_list = json.loads(jsql)
            result = collect.update_many(filter=parm_list[0], update=parm_list[1],
                                         upsert=False, array_filters=None, bypass_document_validation=False, collation=None,
                                         session=None)
            rows_affect = result.modified_count
        elif ope == 'remove':
            if sql != '' and sql != '{}':
                result = collect.remove(spec_or_id=json.loads(sql), multi=True)
                rows_affect = result['n']
        # elif ope == 'delete':
        #     jsql = '[' + sql + ']'
        #     parm_list = json.loads(jsql)
        #     result = collect.delete_one(filter=parm_list[0], collation=None, session=None)
        #     rows_affect = result.deleted_count
        # elif ope == 'deleteMany':
        #     jsql = '[' + sql + ']'
        #     parm_list = json.loads(jsql)
        #     result = collect.delete_many(filter=parm_list[0], collation=None, session=None)
        #     rows_affect = result.deleted_count

        # 此方法有問題
        # else:
        #     req = ope_expression.replace("\'", "\"")
        #     req_list=[]
        #     req_list.append(req)
        #     requests = collect.bulk_write(requests=req_list)
        #     rows_affect = requests.matched_count
        return rows_affect

    @staticmethod
    def _parse_collect_name(db, sql):
        start = sql.find("(")
        ope_expression_pre = sql[0:start]
        ope_expression_split = ope_expression_pre.split('.')
        ope = ope_expression_split[len(ope_expression_split) - 1]
        end = len(ope)
        collect = db[ope_expression_pre[3:len(ope_expression_pre) - end - 1]]
        return collect