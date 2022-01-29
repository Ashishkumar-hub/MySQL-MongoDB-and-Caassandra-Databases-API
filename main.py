import csv
import json
from bson import json_util
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from flask import Flask, render_template, request
import mysql.connector as connection
import pymongo
import ast
import logging
import os

app = Flask(__name__)

c_handler = logging.StreamHandler()
c_handler.setLevel(logging.INFO)
c_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
c_handler.setFormatter(c_format)
logging.basicConfig(filename='test.log', filemode='w+', format='%(asctime)s %(message)s')
lg = logging.getLogger()
lg.addHandler(c_handler)
open(os.getcwd() + 'test.log', 'a')


class DatabaseAPI:
    @staticmethod
    def home_page():
        try:
            return render_template('index.html')
        except Exception as e:
            lg.error(e)
            return render_template('error.html', message="Check logs for more info")

    @staticmethod
    def get_database():
        try:
            if request.method == 'POST':
                db = request.form['db']
                lg.info(db)
                if db == 'mysql':
                    return render_template('establishSqlConnection.html')
                elif db == 'mongodb':
                    return render_template('establishMongoDBConnection.html')
                elif db == 'cassandra':
                    return render_template('establishCassandraConnection.html')
        except Exception as e:
            lg.error(e)
            return render_template('error.html', message="Check logs for more info")

    # -----------------------------------------------------------------------------------------------------------------
    # SQL APIs
    # -----------------------------------------------------------------------------------------------------------------
    @staticmethod
    def sql_connection(host, user, passwd):
        try:
            con = connection.connect(host=host, user=user, passwd=passwd, use_pure=True)
            cur = con.cursor()
            lg.info(str(con) + str(cur))
            return con, cur
        except Exception as e:
            lg.error(e)
            return render_template('error.html', message="Check logs for more info")

    @staticmethod
    def connect_mysql():
        try:
            if request.method == 'POST':
                host = request.form['host']
                user = request.form['user']
                passwd = request.form['passwd']
                lg.info(host + "-" + user)
                con, _ = DatabaseAPI.sql_connection(host, user, passwd)
                if not con.is_connected():
                    raise Exception
                return render_template('selectOperationSql.html', host=host, user=user, passwd=passwd)
        except Exception as e:
            lg.error(e)
            return render_template('error.html', message="Check logs for more info")

    @staticmethod
    def select_sql(host, user, passwd):
        try:
            operation = request.form['operation']
            lg.info(operation)
            if operation == 'create':
                return render_template('createSql.html', host=host, user=user, passwd=passwd)
            elif operation == 'one_insert':
                return render_template('oneInsertSql.html', host=host, user=user, passwd=passwd)
            elif operation == 'bulk_insert':
                return render_template('bulkInsertSql.html', host=host, user=user, passwd=passwd)
            elif operation == 'update':
                return render_template('updateSql.html', host=host, user=user, passwd=passwd)
            elif operation == 'delete':
                return render_template('deleteSql.html', host=host, user=user, passwd=passwd)
            elif operation == 'download':
                return render_template('downloadSql.html', host=host, user=user, passwd=passwd)
        except Exception as e:
            lg.error(e)
            return render_template('error.html', message="Check logs for more info")

    @staticmethod
    def create_table_sql(host, user, passwd):
        try:
            con, cur = DatabaseAPI.sql_connection(host, user, passwd)
            database = request.form['db']
            table = request.form['table']
            attr = request.form['attr']
            cur.execute("CREATE DATABASE IF NOT EXISTS {}".format(database))
            con.commit()
            query = "CREATE TABLE IF NOT EXISTS {}.{} ({})".format(database, table, attr)
            cur.execute(query)
            message = "Table created in MySQL"
            lg.info(database + "-" + table + "-" + attr + "-" + query + "-" + message)
            return render_template('success.html', message=message)
        except Exception as e:
            lg.error(e)
            return render_template('error.html', message="Check logs for more info")

    @staticmethod
    def one_insert_sql(host, user, passwd):
        try:
            con, cur = DatabaseAPI.sql_connection(host, user, passwd)
            database = request.form['db']
            table = request.form['table']
            attr = request.form['attr']
            val = request.form['val']
            if attr == "":
                query = "INSERT INTO {}.{} VALUES({})".format(database, table, val)
            else:
                query = "INSERT INTO {}.{}({}) VALUES({})".format(database, table, attr, val)
            cur.execute(query)
            message = "1 record inserted in MySQL"
            lg.info(database + "-" + table + "-" + attr + "-" + val + "-" + query + "-" + message)
            con.commit()
            return render_template('success.html', message=message)
        except Exception as e:
            lg.error(e)
            return render_template('error.html', message="Check logs for more info")

    @staticmethod
    def bulk_insert_sql(host, user, passwd):
        con, cur = DatabaseAPI.sql_connection(host, user, passwd)
        path = request.form['path']
        database = request.form['db']
        table = request.form['table']
        attr = request.form['attr']
        with open(path, 'r') as data:
            # This code assumes that csv is already parsed
            # next(data) -- Do this if csv contains headers
            # Write code to parse your csv
            data_csv = csv.reader(data, delimiter='\n')
            for i in data_csv:
                row = str(i[0])
                if attr == "":
                    cur.execute("INSERT INTO {}.{} VALUES ({})".format(database, table, row))
                else:
                    cur.execute("INSERT INTO {}.{}({}) VALUES ({})".format(database, table, attr, row))
                con.commit()
        message = "Bulk records inserted in MYSQL"
        lg.info(database + "-" + table + "-" + attr + "-" + message)
        return render_template('success.html', message=message)

    @staticmethod
    def update_sql(host, user, passwd):
        try:
            con, cur = DatabaseAPI.sql_connection(host, user, passwd)
            database = request.form['db']
            table = request.form['table']
            set_attr = request.form['attr']
            where = request.form['val']
            if where == "":
                query = "UPDATE {}.{} SET {}".format(database, table, set_attr)
            else:
                query = "UPDATE {}.{} SET {} WHERE {}".format(database, table, set_attr, where)
            cur.execute(query)
            message = "Record(s) updated in MySQL"
            lg.info(database + "-" + table + "-" + query + "-" + message)
            con.commit()
            return render_template('success.html', message=message)
        except Exception as e:
            lg.error(e)
            return render_template('error.html', message="Check logs for more info")

    @staticmethod
    def delete_sql(host, user, passwd):
        try:
            con, cur = DatabaseAPI.sql_connection(host, user, passwd)
            database = request.form['db']
            table = request.form['table']
            where = request.form['val']
            if where == "":
                query = "DELETE FROM {}.{}".format(database, table)
            else:
                query = "DELETE FROM {}.{} WHERE {}".format(database, table, where)
            cur.execute(query)
            message = "Record(s) deleted in MySQL"
            lg.info(database + "-" + table + "-" + query + "-" + message)
            con.commit()
            return render_template('success.html', message=message)
        except Exception as e:
            lg.error(e)
            return render_template('error.html', message="Check logs for more info")

    @staticmethod
    def download_sql(host, user, passwd):
        try:
            con, cur = DatabaseAPI.sql_connection(host, user, passwd)
            database = request.form['db']
            table = request.form['table']
            path = request.form['path']
            query = "SELECT * FROM {}.{}".format(database, table)
            cur.execute(query)
            open(path, "w+")
            res = cur.fetchall()
            f = open(path, "a")
            for i in res:
                f.write(str(i).replace('(', "").replace(")", "") + "\n")
            f.close()
            message = "Data downloaded from MySQL"
            lg.info(database + "-" + table + "-" + query + "-" + message)
            con.commit()
            return render_template('success.html', message=message)
        except Exception as e:
            lg.error(e)
            return render_template('error.html', message="Check logs for more info")

    # -----------------------------------------------------------------------------------------------------------------
    # MongoDB APIs
    # -----------------------------------------------------------------------------------------------------------------
    @staticmethod
    def mongodb_connection(part1, part2, database, collection):
        try:
            if part2 == "part2":
                con_url = "mongodb://" + part1.replace("_", ":") + "/"
            else:
                con_url = "mongodb+srv://" + part1 + "/" + part2
            client = pymongo.MongoClient(con_url)
            db = client[database]
            collection = db[collection]
            lg.info(con_url)
            return collection
        except Exception as e:
            lg.error(e)
            return render_template('error.html', message="Check logs for more info")

    @staticmethod
    def connect_mongodb():
        try:
            if request.method == 'POST':
                con_url = request.form['con_url']
                parts = con_url[con_url.index(":") + 3:].split('/')
                part1 = parts[0]
                part2 = parts[1]
                if part1.find("localhost") != -1:
                    part1 = "_".join(part1.split(":"))
                    part2 = "part2"
                return render_template('selectOperationMongoDB.html', part1=part1, part2=part2)
        except Exception as e:
            lg.error(e)
            return render_template('error.html', message="Check logs for more info")

    @staticmethod
    def select_mongodb(part1, part2):
        try:
            operation = request.form['operation']
            lg.info(operation)
            if operation == 'create':
                return render_template('createMongoDB.html', part1=part1, part2=part2)
            if operation == 'one_insert':
                return render_template('oneInsertMongoDB.html', part1=part1, part2=part2)
            elif operation == 'bulk_insert':
                return render_template('bulkInsertMongoDB.html', part1=part1, part2=part2)
            elif operation == 'update':
                return render_template('updateMongoDB.html', part1=part1, part2=part2)
            elif operation == 'delete':
                return render_template('deleteMongoDB.html', part1=part1, part2=part2)
            elif operation == 'download':
                return render_template('downloadMongoDB.html', part1=part1, part2=part2)
        except Exception as e:
            lg.error(e)
            return render_template('error.html', message="Check logs for more info")

    @staticmethod
    def create_document_mongodb(part1, part2):
        try:
            database = request.form['db']
            collection = request.form['coll']
            DatabaseAPI.mongodb_connection(part1, part2, database, collection)
            message = "Empty collection created in MongoDB"
            lg.info(database + "-" + str(collection) + "-" + message)
            return render_template('success.html', message=message)
        except Exception as e:
            lg.error(e)
            return render_template('error.html', message="Check logs for more info")

    @staticmethod
    def one_insert_mongodb(part1, part2):
        try:
            database = request.form['db']
            collection = request.form['coll']
            attr_query = ast.literal_eval(request.form['attr'])
            collection = DatabaseAPI.mongodb_connection(part1, part2, database, collection)
            if type(attr_query) == dict:
                collection.insert_one(attr_query)
            elif type(attr_query) == list:
                collection.insert_many(attr_query)
            message = "Record(s) inserted in MongoDB"
            lg.info(database + "-" + str(collection) + "-" + str(attr_query) + "-" + message)
            return render_template('success.html', message=message)
        except Exception as e:
            lg.error(e)
            return render_template('error.html', message="Check logs for more info")

    @staticmethod
    def bulk_insert_mongodb(part1, part2):
        try:
            database = request.form['db']
            collection = request.form['coll']
            path = request.form['path']
            collection = DatabaseAPI.mongodb_connection(part1, part2, database, collection)
            with open(path) as f:
                file_data = ast.literal_eval(json.dumps(json.load(f)))
            if type(file_data) == dict:
                collection.insert_one(file_data)
            elif type(file_data) == list:
                collection.insert_many(file_data)
            message = "Record(s) inserted in MongoDB from json file"
            lg.info(database + "-" + str(collection) + "-" + path + "-" + message)
            return render_template('success.html', message=message)
        except Exception as e:
            lg.error(e)
            return render_template('error.html', message="Check logs for more info")

    @staticmethod
    def update_mongodb(part1, part2):
        try:
            database = request.form['db']
            collection = request.form['coll']
            upd_from = ast.literal_eval(request.form['upd_from'])
            upd_to = ast.literal_eval('{ "$set": ' + request.form['upd_to'] + '}')
            option = request.form['update_op']
            collection = DatabaseAPI.mongodb_connection(part1, part2, database, collection)
            if option == 'update_one':
                collection.update_one(upd_from, upd_to)
            elif option == 'update_many':
                collection.update_many(upd_from, upd_to)
            message = "Record(s) updated in MongoDB"
            lg.info(database + "-" + str(collection) + "-" + str(upd_from) + "-" + str(upd_to) + " " + option + "-" + message)
            return render_template('success.html', message=message)
        except Exception as e:
            lg.error(e)
            return render_template('error.html', message="Check logs for more info")

    @staticmethod
    def delete_mongodb(part1, part2):
        try:
            database = request.form['db']
            collection = request.form['coll']
            delete = ast.literal_eval(request.form['delete'])
            option = request.form['delete_op']
            collection = DatabaseAPI.mongodb_connection(part1, part2, database, collection)
            if option == 'delete_one':
                collection.delete_one(delete)
            elif option == 'delete_many':
                collection.delete_many(delete)
            message = "Record(s) deleted in MongoDB"
            lg.info(database + "-" + str(collection) + "-" + str(delete) + " " + option + "-" + message)
            return render_template('success.html', message=message)
        except Exception as e:
            lg.error(e)
            return render_template('error.html', message="Check logs for more info")

    @staticmethod
    def download_mongodb(part1, part2):
        try:
            database = request.form['db']
            collection = request.form['coll']
            path = request.form['path']
            collection = DatabaseAPI.mongodb_connection(part1, part2, database, collection)
            cur = collection.find()
            list_cur = list(cur)
            json_data = json.loads(json_util.dumps(list_cur))
            with open(path, 'w') as file:
                json.dump(json_data, file, indent=4)
            message = "Data downloaded from MongoDB"
            lg.info(database + "-" + str(collection) + "-" + path + "-" + message)
            return render_template('success.html', message=message)
        except Exception as e:
            lg.error(e)
            return render_template('error.html', message="Check logs for more info")

    # -----------------------------------------------------------------------------------------------------------------
    # Cassandra APIs
    # -----------------------------------------------------------------------------------------------------------------

    @staticmethod
    def cassandra_connection(*args):
        try:
            client_id = args[0]
            client_key = args[1]
            path = args[2]
            keyspace = ""
            class_ = ""
            rf = ""
            session = ""
            if len(args) == 4:
                keyspace = args[3]
            elif len(args) == 5:
                keyspace = args[3]
                class_ = args[4]
            elif len(args) == 6:
                keyspace = args[3]
                class_ = args[4]
                rf = args[5]
            bundle_path = path.replace("/", "****").replace("\\", "****")
            if client_id == "id" and client_key == "key" and bundle_path == "path":
                cluster = Cluster()
                session = cluster.connect()
                return session
            if client_id == "id" and client_key == "key" and bundle_path == "path" and len(args) >= 4:
                cluster = Cluster()
                session = cluster.connect()
                session.execute(
                    "CREATE KEYSPACE IF NOT EXISTS {} WITH replication = {{'class':'{}', 'replication_factor' : {}}}".format(
                        keyspace,
                        class_,
                        rf))
                return session
            if bundle_path != "path":
                cloud_config = {
                    'secure_connect_bundle': bundle_path.replace("****", "/")
                }
                auth_provider = PlainTextAuthProvider(client_id, client_key)
                cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
                session = cluster.connect()
                return session
            lg.info("-".join(args) + "-" + str(session))
            return session
        except Exception as e:
            lg.error(e)
            return render_template('error.html', message="Check logs for more info")

    @staticmethod
    def connect_cassandra():
        try:
            if request.method == 'POST':
                client_id = request.form["id"]
                client_key = request.form["key"]
                path = request.form["path"]
                if path == "" or client_key == "" or client_id == "":
                    path = "path"
                    client_key = "key"
                    client_id = "id"
                bundle_path = path.replace("/", "****").replace("\\", "****")
                session = DatabaseAPI.cassandra_connection(client_id, client_key, bundle_path)
                if type(session) == str:
                    raise Exception
                else:
                    return render_template('selectOperationCassandra.html', client_id=client_id, client_key=client_key,
                                           bundle_path=bundle_path)
        except Exception as e:
            lg.error(e)
            return render_template('error.html', message="Check logs for more info")

    @staticmethod
    def select_cassandra(client_id, client_key, bundle_path):
        try:
            operation = request.form['operation']
            lg.info(operation)
            if operation == 'create':
                return render_template('createCassandra.html', client_id=client_id, client_key=client_key,
                                       bundle_path=bundle_path)
            elif operation == 'one_insert':
                return render_template('oneInsertCassandra.html', client_id=client_id, client_key=client_key,
                                       bundle_path=bundle_path)
            elif operation == 'bulk_insert':
                return render_template('bulkInsertCassandra.html', client_id=client_id, client_key=client_key,
                                       bundle_path=bundle_path)
            elif operation == 'update':
                return render_template('updateCassandra.html', client_id=client_id, client_key=client_key,
                                       bundle_path=bundle_path)
            elif operation == 'delete':
                return render_template('deleteCassandra.html', client_id=client_id, client_key=client_key,
                                       bundle_path=bundle_path)
            elif operation == 'download':
                return render_template('downloadCassandra.html', client_id=client_id, client_key=client_key,
                                       bundle_path=bundle_path)
        except Exception as e:
            lg.error(e)
            return render_template('error.html', message="Check logs for more info")

    @staticmethod
    def create_table_cassandra(client_id, client_key, bundle_path):
        try:
            keyspace = request.form['db']
            class_ = request.form['class']
            rf = request.form['rf']
            table = request.form['table']
            attr = request.form['attr']
            session = DatabaseAPI.cassandra_connection(client_id, client_key, bundle_path, keyspace, class_, rf)
            session.execute("USE {}".format(keyspace))
            query = "CREATE TABLE IF NOT EXISTS {}.{} ({})".format(keyspace, table, attr)
            session.execute(query)
            message = "Table created in Cassandra"
            lg.info(keyspace + "-" + class_ + "-" + rf + "-" + table + " " + attr + "-" + query + "-" + message)
            return render_template('success.html', message=message)
        except Exception as e:
            lg.error(e)
            return render_template('error.html', message="Check logs for more info")

    @staticmethod
    def one_insert_cassandra(client_id, client_key, bundle_path):
        try:
            keyspace = request.form['db']
            table = request.form['table']
            attr = request.form['attr']
            val = request.form['val']
            session = DatabaseAPI.cassandra_connection(client_id, client_key, bundle_path, keyspace)
            session.execute("INSERT INTO {}.{}({}) VALUES ({}) ".format(keyspace, table, attr, val))
            message = "1 record inserted in Cassandra"
            lg.info(keyspace + "-" + table + " " + attr + "-" + message)
            return render_template('success.html', message=message)
        except Exception as e:
            lg.error(e)
            return render_template('error.html', message="Check logs for more info")

    @staticmethod
    def bulk_insert_cassandra(client_id, client_key, bundle_path):
        keyspace = request.form['db']
        table = request.form['table']
        attr = request.form['attr']
        path = request.form['path']
        session = DatabaseAPI.cassandra_connection(client_id, client_key, bundle_path, keyspace)
        # This code assumes that csv is already parsed
        # next(data) -- Do this if csv contains headers
        # Write code to parse your csv
        with open(path, 'r') as data:
            data_csv = csv.reader(data, delimiter='\n')
            for i in data_csv:
                row = str(i[0])
                query = "INSERT INTO {}.{}({}) VALUES ({}) ".format(keyspace, table, attr, row)
                session.execute(query)
        message = "Bulk records inserted in Cassandra"
        lg.info(keyspace + "-" + path + "-" + table + " " + attr + "-" + query + "-" + message)
        return render_template('success.html', message=message)
        # except Exception as e:
        #     lg.error(e)
        #     return render_template('error.html', message="Check logs for more info")

    @staticmethod
    def update_cassandra(client_id, client_key, bundle_path):
        try:
            keyspace = request.form['db']
            table = request.form['table']
            set_attr = request.form['attr']
            where = request.form['val']
            session = DatabaseAPI.cassandra_connection(client_id, client_key, bundle_path, keyspace)
            if where == "":
                query = "UPDATE {}.{} SET {}".format(keyspace, table, set_attr)
            else:
                query = "UPDATE {}.{} SET {} WHERE {}".format(keyspace, table, set_attr, where)
            session.execute(query)
            message = "Record(s) updated in Cassandra"
            lg.info(keyspace + "-" + where + "-" + table + " " + set_attr + "-" + query + "-" + message)
            return render_template('success.html', message=message)
        except Exception as e:
            lg.error(e)
            return render_template('error.html', message="Check logs for more info")

    @staticmethod
    def delete_cassandra(client_id, client_key, bundle_path):
        try:
            keyspace = request.form['db']
            table = request.form['table']
            where = request.form['val']
            session = DatabaseAPI.cassandra_connection(client_id, client_key, bundle_path, keyspace)
            if where == "":
                query = "DELETE FROM {}.{}".format(keyspace, table)
            else:
                query = "DELETE FROM {}.{} WHERE {}".format(keyspace, table, where)
            session.execute(query)
            message = "Record(s) deleted in Cassandra"
            lg.info(keyspace + "-" + table + " " + where + "-" + query + "-" + message)
            return render_template('success.html', message=message)
        except Exception as e:
            lg.error(e)
            return render_template('error.html', message="Check logs for more info")

    @staticmethod
    def download_cassandra(client_id, client_key, bundle_path):
        try:
            keyspace = request.form['db']
            table = request.form['table']
            path = request.form['path']
            query = "SELECT * FROM {}.{}".format(keyspace, table)
            column_query = "SELECT * FROM system_schema.columns WHERE keyspace_name = '{}' and table_name= '{}'".format(
                keyspace, table)
            session = DatabaseAPI.cassandra_connection(client_id, client_key, bundle_path, keyspace)
            rows = session.execute(query)
            col_rows = session.execute(column_query)
            cols = []
            for i in col_rows:
                cols.append(i[2])
            open(path, "w+")
            f = open(path, "a")
            for i in rows:
                row_list = str(i).split(",")
                middle = []
                first = ""
                last = ""
                for j in range(0, len(row_list)):
                    if j == 0:
                        first = str(row_list[j]).replace("Row(" + cols[j] + "=", "")
                    elif 1 <= j < len(row_list) - 1:
                        middle.append(str(row_list[j]).replace(cols[j] + "=", ""))
                    elif j == len(row_list) - 1:
                        last = str(row_list[j]).replace(")", "").replace(cols[j] + "=", "")
                str_middle = ",".join(middle)
                f.write(first + "," + str_middle + last + "\n")
            f.close()
            message = "Data downloaded from Cassandra"
            lg.info(keyspace + "-" + table + " " + path + "-" + query + "-" + message)
            return render_template('success.html', message=message)
        except Exception as e:
            lg.error(e)
            return render_template('error.html', message="Check logs for more info")


try:
    app.add_url_rule('/establishConnection', view_func=DatabaseAPI.get_database, methods=['GET', 'POST'])

    # -----------------------------------------------------------------------------------------------------------------
    # app URL route registrations -- SQL
    # -----------------------------------------------------------------------------------------------------------------
    app.add_url_rule('/', view_func=DatabaseAPI.home_page, methods=['GET', 'POST'])
    app.add_url_rule('/establishSqlConnection', view_func=DatabaseAPI.connect_mysql, methods=['GET', 'POST'])
    app.add_url_rule('/selectOperationSql/<host>/<user>/<passwd>', view_func=DatabaseAPI.select_sql,
                     methods=['GET', 'POST'])
    app.add_url_rule('/createTableSql/<host>/<user>/<passwd>', view_func=DatabaseAPI.create_table_sql,
                     methods=['GET', 'POST'])
    app.add_url_rule('/oneInsertSql/<host>/<user>/<passwd>', view_func=DatabaseAPI.one_insert_sql,
                     methods=['GET', 'POST'])
    app.add_url_rule('/bulkInsertSql/<host>/<user>/<passwd>', view_func=DatabaseAPI.bulk_insert_sql,
                     methods=['GET', 'POST'])
    app.add_url_rule('/updateSql/<host>/<user>/<passwd>', view_func=DatabaseAPI.update_sql,
                     methods=['GET', 'POST'])
    app.add_url_rule('/deleteSql/<host>/<user>/<passwd>', view_func=DatabaseAPI.delete_sql,
                     methods=['GET', 'POST'])
    app.add_url_rule('/downloadSql/<host>/<user>/<passwd>', view_func=DatabaseAPI.download_sql,
                     methods=['GET', 'POST'])

    # -----------------------------------------------------------------------------------------------------------------
    # app URL route registrations -- MongoDB
    # -----------------------------------------------------------------------------------------------------------------
    app.add_url_rule('/establishMongoDBConnection', view_func=DatabaseAPI.connect_mongodb,
                     methods=['GET', 'POST'])
    app.add_url_rule('/selectOperationMongoDB/<part1>/<part2>', view_func=DatabaseAPI.select_mongodb,
                     methods=['GET', 'POST'])
    app.add_url_rule('/createDocumentMongoDB/<part1>/<part2>', view_func=DatabaseAPI.create_document_mongodb,
                     methods=['GET', 'POST'])
    app.add_url_rule('/oneInsertMongoDB/<part1>/<part2>', view_func=DatabaseAPI.one_insert_mongodb,
                     methods=['GET', 'POST'])
    app.add_url_rule('/bulkInsertMongoDB/<part1>/<part2>', view_func=DatabaseAPI.bulk_insert_mongodb,
                     methods=['GET', 'POST'])
    app.add_url_rule('/updateMongoDB/<part1>/<part2>', view_func=DatabaseAPI.update_mongodb,
                     methods=['GET', 'POST'])
    app.add_url_rule('/deleteMongoDB/<part1>/<part2>', view_func=DatabaseAPI.delete_mongodb,
                     methods=['GET', 'POST'])
    app.add_url_rule('/downloadMongoDB/<part1>/<part2>', view_func=DatabaseAPI.download_mongodb,
                     methods=['GET', 'POST'])

    # -----------------------------------------------------------------------------------------------------------------
    # app URL route registrations -- Cassandra
    # -----------------------------------------------------------------------------------------------------------------
    app.add_url_rule('/establishCassandraConnection', view_func=DatabaseAPI.connect_cassandra,
                     methods=['GET', 'POST'])
    app.add_url_rule('/selectOperationCassandra/<client_id>/<client_key>/<bundle_path>',
                     view_func=DatabaseAPI.select_cassandra,
                     methods=['GET', 'POST'])
    app.add_url_rule('/createTableCassandra/<client_id>/<client_key>/<bundle_path>',
                     view_func=DatabaseAPI.create_table_cassandra,
                     methods=['GET', 'POST'])
    app.add_url_rule('/oneInsertCassandra/<client_id>/<client_key>/<bundle_path>',
                     view_func=DatabaseAPI.one_insert_cassandra,
                     methods=['GET', 'POST'])
    app.add_url_rule('/bulkInsertCassandra/<client_id>/<client_key>/<bundle_path>',
                     view_func=DatabaseAPI.bulk_insert_cassandra,
                     methods=['GET', 'POST'])
    app.add_url_rule('/updateCassandra/<client_id>/<client_key>/<bundle_path>', view_func=DatabaseAPI.update_cassandra,
                     methods=['GET', 'POST'])
    app.add_url_rule('/deleteCassandra/<client_id>/<client_key>/<bundle_path>', view_func=DatabaseAPI.delete_cassandra,
                     methods=['GET', 'POST'])
    app.add_url_rule('/downloadCassandra/<client_id>/<client_key>/<bundle_path>',
                     view_func=DatabaseAPI.download_cassandra,
                     methods=['GET', 'POST'])
except Exception as e:
    lg.error(e)
    render_template('error.html', message="Check logs for more info")

if __name__ == '__main__':
    app.run()
