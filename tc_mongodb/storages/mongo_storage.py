# -*- coding: utf-8 -*-
# Licensed under the MIT license:
# http://www.opensource.org/licenses/mit-license
# Copyright (c) 2015 Thumbor-Community
# Copyright (c) 2011 globo.com timehome@corp.globo.com

from datetime import datetime, timedelta
from cStringIO import StringIO

# https://api.mongodb.com/python/current/tutorial.html
from pymongo import MongoClient
import pymongo
import gridfs

from thumbor.storages import BaseStorage
from tornado.concurrent import return_future

def deleteDataList(db, dictThumborToMongo ,removeListDictDatas):
    mongoFileMetadata = db['fs.files']
    mongoBinaryStorage = db['fs.chunks']

    for dictData in removeListDictDatas:
        docGridFSchunks = {
            'files_id': dictData['file_id']
        }
        mongoGridFsQuery={
            '_id': dictData['file_id']
        }
        dictThumborToMongo.delete_many(dictData)
        mongoFileMetadata.delete_many(mongoGridFsQuery)
        mongoBinaryStorage.delete_many(docGridFSchunks)

class Storage(BaseStorage):

    def __conn__(self):
        # https://docs.mongodb.com/manual/reference/connection-string/
        connection = MongoClient(
            self.context.config.MONGO_STORAGE_SERVER_URI
        )

        db = connection[self.context.config.MONGO_STORAGE_SERVER_DB]
        dictThumborToMongo = db[self.context.config.MONGO_STORAGE_SERVER_COLLECTION]

        dictThumborToMongo.create_index([('path',pymongo.ASCENDING)],unique=True)

        return connection, db, dictThumborToMongo

    def put(self, path, bytes):
        connection, db, dictThumborToMongo = self.__conn__()

        oldDictDatas=[]
        mongoBinaryData=[]
        #Before my Upload https://docs.mongodb.com/manual/core/gridfs/
        mongoBinaryStorage = db['fs.chunks']
        mongoFileMetadata = db['fs.files']
        oldDictDatas = dictThumborToMongo.find({'path': { '$regex': path }})
        if oldDictDatas:
            for dictData in oldDictDatas:
                pass

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

        if oldDictDatas:
            deleteDataList(db,dictThumborToMongo,oldDictDatas)

        return path

    def put_crypto(self, path):
        if not self.context.config.STORES_CRYPTO_KEY_FOR_EACH_IMAGE:
            return

        connection, db, dictThumborToMongo = self.__conn__()

        if not self.context.server.security_key:
            raise RuntimeError("STORES_CRYPTO_KEY_FOR_EACH_IMAGE can't be True if no SECURITY_KEY specified")

        cryptos = dictThumborToMongo.find({'path': { '$regex': path }})
        for crypto in cryptos:
            cryptoOld = crypto.copy()
            crypto['crypto'] = self.context.server.security_key
            dictThumborToMongo.update(cryptoOld, crypto)
        return path

    def put_detector_data(self, path, data):
        connection, db, dictThumborToMongo = self.__conn__()

        dictThumborToMongo.update({'path': path}, {"$set": {"detector_data": data}})
        return path

    @return_future
    def get_crypto(self, path, callback):
        connection, db, dictThumborToMongo = self.__conn__()

        crypto = dictThumborToMongo.find({'path': path})
        if not crypto:
            callback(None)
        callback(crypto[0].get('crypto') if crypto else None)

    @return_future
    def get_detector_data(self, path, callback):
        connection, db, dictThumborToMongo = self.__conn__()

        doc = dictThumborToMongo.find({'path': path})
        if not doc:
            callback(None)
        callback(doc[0].get('detector_data') if doc else None)

    @return_future
    def get(self, path, callback):
        connection, db, dictThumborToMongo = self.__conn__()

        dictData = dictThumborToMongo.find({'path': path})
        if not dictData:
            callback(None)

        if not dictData[0] or self.__is_expired(dictData[0]):
            callback(None)
            return

        fs = gridfs.GridFS(db)

        contents = fs.get(dictData[0]['file_id']).read()

        callback(str(contents))

    @return_future
    def exists(self, path, callback):
        connection, db, dictThumborToMongo = self.__conn__()

        dictData = dictThumborToMongo.find({'path': path})
        if not dictData:
            callback(False)

        if not dictData[0] or self.__is_expired(dictData[0]):
            callback(False)
        else:
            callback(True)

    def remove(self, path):
        connection, db, dictThumborToMongo = self.__conn__()

        mongoFileMetadata = db['fs.files']
        mongoBinaryStorage = db['fs.chunks']

        #Q find id
        removeListDictDatas = dictThumborToMongo.find({'path': { '$regex': path }})
        deleteDataList(db,dictThumborToMongo,removeListDictDatas)

    def __is_expired(self, dictData):
        timediff = datetime.now() - dictData.get('created_at')
        return timediff > timedelta(seconds=self.context.config.STORAGE_EXPIRATION_SECONDS)
        #return False
