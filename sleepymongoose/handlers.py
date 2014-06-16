import tornado.web

from bson.son import SON
from pymongo import Connection, ASCENDING, DESCENDING
from pymongo.errors import ConnectionFailure, ConfigurationError, OperationFailure, AutoReconnect
from bson import json_util

import re
try:
    import json
except ImportError:
    import simplejson as json


class MongorestHandler(tornado.web.RequestHandler):

    def initialize(self, xorigin):

        self.set_header('Access-Control-Allow-Origin', xorigin)
        self.set_header('Access-Control-Allow-Methods', 'GET, PUT, POST, DELETE, OPTIONS')
        self.set_header('Content-Type', 'application/json')

    def options(self, db, collection):
        pass

    def get(self, db, collection):

        if db == None or collection == None:
            return self.write('{"ok" : 0, "errmsg" : "db and collection must be defined"}')

        if collection == 'hello':
            return self.write('{"ok" : 1, "msg" : "Uh, we had a slight weapons malfunction, but ' +
                'uh... everything\'s perfectly all right now. We\'re fine. We\'re ' +
                'all fine here now, thank you. How are you?"}')

        if collection == 'status':
            result = {"ok" : 1, "connections" : {}}
            for name, conn in self.connections.iteritems():
                result['connections'][name] = "%s:%d" % (conn.host, conn.port)

            return self.write(json.dumps(result))

#        be carefule to using this function!!! it can drop database and shutdown the server!!!

#        if collection == 'run':
#            name = self.get_argument('name', None)

#            conn = self.application._get_connection(name)
#            if conn == None:
#                return self.write('{"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}')

#            cmd = self._get_son('cmd')
#            if cmd == None:
#                return

#            try:
#                result = conn[db].command(cmd, check=False)
#            except AutoReconnect:
#                return self.write('{"ok" : 0, "errmsg" : "wasn\'t connected to the db and '+
#                    'couldn\'t reconnect", "name" : "%s"}' % name)
#            except (OperationFailure, error):
#                return self.write('{"ok" : 0, "errmsg" : "%s"}' % error)

#            if result['ok'] == 0:
#                result['cmd'] = cmd

#            return self.write(json.dumps(result, default=json_util.default))

        if collection == 'more':
            # Get more results from a cursor
            if not self.get_argument('id'):
                return self.write('{"ok" : 0, "errmsg" : "no cursor id given"}')

            cid = int(self.get_argument('id'))
            cursor = self.application.cursors.get(cid)
            if not cursor:
                return self.write('{"ok" : 0, "errmsg" : "couldn\'t find the cursor with id %d"}' % cid)

        else:
            # query the database
            name = self.get_argument('name', None)

            criteria = self._get_son('criteria') or {}
            fields = self._get_son('fields') or None
            limit = int(self.get_argument('limit', 0)) or 0
            skip = int(self.get_argument('skip', 0)) or 0

            conn = self.application._get_connection(name)
            if conn == None:
                return self.write('{"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}')

            cursor = conn[db][collection].find(spec=criteria, fields=fields, limit=limit, skip=skip)

            if self.get_argument('sort', None):
                sort = self._get_son('sort') or {}
                stupid_sort = [[f, sort[f] == -1 and DESCENDING or ASCENDING] for f in sort]
                cursor.sort(stupid_sort)

            if bool(self.get_argument('explain', False)):
                return self.write(json.dumps({"results" : [cursor.explain()], "ok" : 1}, default=json_util.default))

            cid = self.application._cursor_id
            self.application._cursor_id = self.application._cursor_id + 1

            if len(self.application.cursors) >= 1000:
                self.application.cursors = {}

            self.application.cursors[cid] = cursor

        batch_size = int(self.get_argument('batch_size', 15))

        self.__output_results(cursor, batch_size, cid)

    def put(self, db, collection):
        """
        insert a doc
        """
        if db == None or collection == None:
            return self.write('{"ok" : 0, "errmsg" : "db and collection must be defined"}')

        conn = self.application._get_connection(self.get_argument('name', None))
        if conn == None:
            return self.write('{"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}')

        if not self.get_argument('docs'):
            return self.write('{"ok" : 0, "errmsg" : "missing docs"}')

        docs = self._get_son('docs')
        if docs == None:
            return

        result = {'oids': conn[db][collection].insert(docs)}
        if self.get_argument('safe', False):
            result['status'] = conn[db].last_status()

        self.write(json.dumps(result, default=json_util.default))


    def post(self, db, collection):
        #authenticate to the database.
        if db == None or collection == None:
            return self.write('{"ok" : 0, "errmsg" : "db and collection must be defined"}')

        conn = self.application._get_connection(self.get_argument('name', None))
        if conn == None:
            return self.write('{"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}')

        if collection == 'authenticate':

            username = self.get_argument('username', None)
            password = self.get_argument('password', None)

            if not (username and password):
                return self.write('{"ok" : 0, "errmsg" : "username and password must be defined"}')

            if not conn[db].authenticate(args.getvalue('username'), args.getvalue('password')):
                return self.write('{"ok" : 0, "errmsg" : "authentication failed"}')
            else:
                return self.write('{"ok" : 1}')
        else:

            # update a doc

            criteria = self._get_son('criteria')
            newobj = self._get_son('newobj')
            if not (criteria and newobj):
                return self.write('{"ok": 0, "errmsg": "missing criteria or newobj"}')

            upsert = bool(self.get_argument('upsert', False))
            multi = bool(self.get_argument('multi', False))

            conn[db][collection].update(criteria, newobj, upsert=upsert, multi=multi)

            self.__safety_check(conn[db])


    def delete(self, db, collection):
        """
        remove docs
        """
        if db == None or collection == None:
            return self.write('{"ok" : 0, "errmsg" : "db and collection must be defined"}')

        conn = self.application._get_connection(self.get_argument('name', None))
        if conn == None:
            return self.write('{"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}')

        criteria = self._get_son('criteria')
        if criteria and len(criteria) > 0:
            result = conn[db][collection].remove(criteria)

            self.__safety_check(conn[db])

    def sm_object_hook(obj):
        if "$pyhint" in obj:
            temp = SON()
            for pair in obj['$pyhint']:
                temp[pair['key']] = pair['value']
            return temp
        else:
            return json_util.object_hook(obj)

    def _get_son(self, name):
        try:
            json_str = self.get_argument(name, '{}')
            obj = json.loads(json_str, object_hook=json_util.object_hook)
        except (ValueError, TypeError):
            print 'couldn\'t parse json: %s' % json_str
            return None

        if getattr(obj, '__iter__', False) == False:
            print 'type is not iterable: %s' % json_str
            return None
        return obj

    def __output_results(self, cursor, batch_size=15, cid=None):
        """
        Iterate through the next batch
        """
        batch = []

        try:
            while len(batch) < batch_size:
                batch.append(cursor.next())
        except AutoReconnect:
            self.write(json.dumps({"ok" : 0, "errmsg" : "auto reconnecting, please try again"}))
            return
        except OperationFailure, of:
            self.write(json.dumps({"ok" : 0, "errmsg" : "%s" % of}))
            return
        except StopIteration:
            # this is so stupid, there's no has_next?
            pass

        self.write(json.dumps({"results" : batch, "id" : cid, "ok" : 1}, default=json_util.default))

    def __safety_check(self, db):

        safe = bool(self.get_argument('safe', False))

        if safe:
            result = db.last_status()
            self.write(json.dumps(result, default=json_util.default))
        else:
            self.write('{"ok" : 1}')


class GridfsHandler(MongorestHandler):

    def get(self, db):
        pass
    def put(self, db):
        pass
    def post(self, db):
        pass
    def delete(self, db):
        pass
