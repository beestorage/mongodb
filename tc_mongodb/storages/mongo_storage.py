# -*- coding: utf-8 -*-
# Licensed under the MIT license:
# http://www.opensource.org/licenses/mit-license
# Copyright (c) 2015 Thumbor-Community
# Copyright (c) 2011 globo.com timehome@corp.globo.com

from datetime import datetime, timedelta
from cStringIO import StringIO

# https://api.mongodb.com/python/current/tutorial.html
from pymongo import MongoClient
import gridfs

from thumbor.storages import BaseStorage
from tornado.concurrent import return_future


class Storage(BaseStorage):

    def __conn__(self):
        # https://docs.mongodb.com/manual/reference/connection-string/
        connection = MongoClient(
            self.context.config.MONGO_STORAGE_SERVER_URI
        )

        db = connection[self.context.config.MONGO_STORAGE_SERVER_DB]
        dictThumborToMongo = db[self.context.config.MONGO_STORAGE_SERVER_COLLECTION]

        return connection, db, dictThumborToMongo

    def put(self, path, bytes):
        connection, db, dictThumborToMongo = self.__conn__()

        dictData=[]
        mongoBinaryData=[]
        #Before my Upload https://docs.mongodb.com/manual/core/gridfs/
        mongoBinaryStorage = db['fs.chunks']
        mongoFileMetadata = db['fs.files']
        dictData = dictThumborToMongo.find_one({'path': path})

        if dictData:
            mongoBinaryData = mongoBinaryStorage.find({'files_id': dictData['file_id']})

        doc = {
            'path': path,
            'created_at': datetime.now()
        }

        doc_with_crypto = dict(doc)
        # Check set STORES_CRYPTO_KEY_FOR_EACH_IMAGE
        if self.context.config.STORES_CRYPTO_KEY_FOR_EACH_IMAGE:
            # Check security_key https://github.com/thumbor/thumbor/wiki/security#hmac-method
            if not self.context.server.security_key:
                raise RuntimeError("STORES_CRYPTO_KEY_FOR_EACH_IMAGE can't be True if no SECURITY_KEY specified")
            doc_with_crypto['crypto'] = self.context.server.security_key

        fs = gridfs.GridFS(db)
        file_data_id = fs.put(StringIO(bytes), **doc)

        doc_with_crypto['file_id'] = file_data_id
        dictThumborToMongo.insert(doc_with_crypto)


        if dictData:
            for docC in mongoBinaryData:
                mongoBinaryStorage.delete_many({'_id': docC['_id']})
            mongoGridFsQuery={
                '_id': dictData['file_id']
            }
            mongoDictQuery = {
                '_id': dictData['_id']
            }
            dictThumborToMongo.delete_many(mongoDictQuery)
            mongoFileMetadata.delete_many(mongoGridFsQuery)
            # TODO check remove binary file

        return path

    def put_crypto(self, path):
        if not self.context.config.STORES_CRYPTO_KEY_FOR_EACH_IMAGE:
            return

        connection, db, dictThumborToMongo = self.__conn__()

        if not self.context.server.security_key:
            raise RuntimeError("STORES_CRYPTO_KEY_FOR_EACH_IMAGE can't be True if no SECURITY_KEY specified")

        crypto = dictThumborToMongo.find_one({'path': path})

        crypto['crypto'] = self.context.server.security_key
        dictThumborToMongo.update({'path': path}, crypto)
        return path

    def put_detector_data(self, path, data):
        connection, db, dictThumborToMongo = self.__conn__()

        dictThumborToMongo.update({'path': path}, {"$set": {"detector_data": data}})
        return path

    @return_future
    def get_crypto(self, path, callback):
        connection, db, dictThumborToMongo = self.__conn__()

        crypto = dictThumborToMongo.find_one({'path': path})
        callback(crypto.get('crypto') if crypto else None)

    @return_future
    def get_detector_data(self, path, callback):
        connection, db, dictThumborToMongo = self.__conn__()

        doc = dictThumborToMongo.find_one({'path': path})
        callback(doc.get('detector_data') if doc else None)

    @return_future
    def get(self, path, callback):
        connection, db, dictThumborToMongo = self.__conn__()

        dictData = dictThumborToMongo.find_one({'path': path})

        if not dictData or self.__is_expired(dictData):
            callback(None)
            return

        fs = gridfs.GridFS(db)

        contents = fs.get(dictData['file_id']).read()

        callback(str(contents))

    @return_future
    def exists(self, path, callback):
        connection, db, dictThumborToMongo = self.__conn__()

        dictData = dictThumborToMongo.find_one({'path': path})

        if not dictData or self.__is_expired(dictData):
            callback(False)
        else:
            callback(True)

    def remove(self, path):
        connection, db, dictThumborToMongo = self.__conn__()

        mongoFileMetadata = db['fs.files']
        mongoBinaryStorage = db['fs.chunks']

        #Q find id
        dictData = dictThumborToMongo.find_one({'path': path})

        docGridFSchunks = {
            'files_id': dictData['file_id']
        }
        mongoGridFsQuery={
            '_id': dictData['file_id']
        }
        mongoDictQuery = {
            'path': path
        }
        dictThumborToMongo.delete_many(mongoDictQuery)
        mongoFileMetadata.delete_many(mongoGridFsQuery)
        mongoBinaryStorage.delete_many(docGridFSchunks)

    def __is_expired(self, dictData):
        timediff = datetime.now() - dictData.get('created_at')
        return timediff > timedelta(seconds=self.context.config.STORAGE_EXPIRATION_SECONDS)
        #return False
