# tc_mongodb
[![CircleCI](https://circleci.com/gh/beestorage/mongodb.svg?style=svg)](https://circleci.com/gh/beestorage/mongodb)  
MongoDB storage adapter for thumbor.

# Versions

This projects uses the following versioning scheme:

`<thumbor major>.<mongodb plugin major>.<mongodb plugin minor>`


# Configuration
```
# MONGO STORAGE OPTIONS
MONGO_STORAGE_SERVER_URI = 'mongodb://mongodb0.example.com:27017/admin' # MongoDB storage server URI  
MONGO_STORAGE_SERVER_DB = 'admin'
MONGO_STORAGE_SERVER_COLLECTION = 'images' # MongoDB storage image collection
```

[Ref. Mongo Server URI](https://docs.mongodb.com/manual/reference/connection-string/)  

[Setup mongo user account option](https://zocada.com/setting-mongodb-users-beginners-guide)
