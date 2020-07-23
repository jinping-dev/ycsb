/**
 * Copyright (c) 2012 - 2015 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

/*
 * MongoDB client binding for YCSB.
 *
 * Submitted by Yen Pai on 5/11/2010.
 *
 * https://gist.github.com/000a66b8db2caf42467b#file_mongo_database.java
 */
package site.ycsb.db;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

import org.bson.Document;
import org.bson.types.Binary;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * MongoDB binding for YCSB framework using the MongoDB Inc. <a
 * href="http://docs.mongodb.org/ecosystem/drivers/java/">driver</a>
 * <p>
 * See the <code>README.md</code> for configuration information.
 * </p>
 *
 * @author ypai
 * @see <a href="http://docs.mongodb.org/ecosystem/drivers/java/">MongoDB Inc.
 *      driver</a>
 */
public class MongoDbClient extends DB {

  /** Used to include a field in a response. */
  private static final Integer INCLUDE = Integer.valueOf(1);

  /** The options to use for inserting many documents. */
  private static final InsertManyOptions INSERT_UNORDERED =
      new InsertManyOptions().ordered(false);

  /** The options to use for inserting a single document. */
  private static final UpdateOptions UPDATE_WITH_UPSERT = new UpdateOptions()
      .upsert(true);

  /**
   * The database name to access.
   */
  private static String databaseName;

  /** The database name to access. */
  private static MongoDatabase database;

  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  /** A singleton Mongo instance. */
  private static MongoClient mongoClient;

  /** The default read preference for the test. */
  private static ReadPreference readPreference;

  /** The default write concern for the test. */
  private static WriteConcern writeConcern;

  /** The batch size to use for inserts. */
  private static int batchSize;

  /** If true then use updates with the upsert option for inserts. */
  private static boolean useUpsert;

  /** The bulk inserts pending for the thread. */
  private final List<Document> bulkInserts = new ArrayList<Document>();


  private static volatile AtomicInteger liveId = new AtomicInteger(10000000);
  private static volatile AtomicInteger userId = new AtomicInteger(1000000);

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB
   * instance per client thread.
   */
  @Override
  public void cleanup() throws DBException {
    if (INIT_COUNT.decrementAndGet() == 0) {
      try {
        mongoClient.close();
      } catch (Exception e1) {
        System.err.println("Could not close MongoDB connection pool: "
            + e1.toString());
        e1.printStackTrace();
        return;
      } finally {
        database = null;
        mongoClient = null;
      }
    }
  }

  /**
   * Delete a record from the database.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error. See the {@link DB}
   *         class's description for a discussion of error codes.
   */
  @Override
  public Status delete(String table, String key) {
    try {
      MongoCollection<Document> collection = database.getCollection(table);

      Document query = new Document("_id", key);
      DeleteResult result =
          collection.withWriteConcern(writeConcern).deleteOne(query);
      if (result.wasAcknowledged() && result.getDeletedCount() == 0) {
        System.err.println("Nothing deleted for key " + key);
        return Status.NOT_FOUND;
      }
      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    }
  }

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    INIT_COUNT.incrementAndGet();
    synchronized (INCLUDE) {
      if (mongoClient != null) {
        return;
      }

      Properties props = getProperties();

      // Set insert batchsize, default 1 - to be YCSB-original equivalent
      batchSize = Integer.parseInt(props.getProperty("batchsize", "1"));

      // Set is inserts are done as upserts. Defaults to false.
      useUpsert = Boolean.parseBoolean(
          props.getProperty("mongodb.upsert", "false"));

      // Just use the standard connection format URL
      // http://docs.mongodb.org/manual/reference/connection-string/
      // to configure the client.
      String url = props.getProperty("mongodb.url", null);
      boolean defaultedUrl = false;
      if (url == null) {
        defaultedUrl = true;
        url = "mongodb://localhost:27017/ycsb?w=1";
      }

      url = OptionsSupport.updateUrl(url, props);

      if (!url.startsWith("mongodb://") && !url.startsWith("mongodb+srv://")) {
        System.err.println("ERROR: Invalid URL: '" + url
            + "'. Must be of the form "
            + "'mongodb://<host1>:<port1>,<host2>:<port2>/database?options' "
            + "or 'mongodb+srv://<host>/database?options'. "
            + "http://docs.mongodb.org/manual/reference/connection-string/");
        System.exit(1);
      }

      try {
        MongoClientURI uri = new MongoClientURI(url);

        String uriDb = uri.getDatabase();
        if (!defaultedUrl && (uriDb != null) && !uriDb.isEmpty()
            && !"admin".equals(uriDb)) {
          databaseName = uriDb;
        } else {
          // If no database is specified in URI, use "ycsb"
          databaseName = "ycsb";

        }

        readPreference = uri.getOptions().getReadPreference();
        writeConcern = uri.getOptions().getWriteConcern();

        mongoClient = new MongoClient(uri);
        database =
            mongoClient.getDatabase(databaseName)
                .withReadPreference(readPreference)
                .withWriteConcern(writeConcern);

        System.out.println("mongo client connection created with " + url);
      } catch (Exception e1) {
        System.err
            .println("Could not initialize MongoDB connection pool for Loader: "
                + e1.toString());
        e1.printStackTrace();
        return;
      }
    }
  }




  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to insert.
   * @param values
   *          A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error. See the {@link DB}
   *         class's description for a discussion of error codes.
   */
//  @Override
//  public Status insert(String table, String key,
//                       Map<String, ByteIterator> values) {
//    try {
//      MongoCollection<Document> collection = database.getCollection(table);
//      Random random = new Random();
//      int liveIdInteger = liveId.incrementAndGet();
//
//      Document toInsert = new Document("_id", UUID.randomUUID().toString());
//      toInsert.put("live_id", liveIdInteger);
//      toInsert.put("live_mode", "live");
//      toInsert.put("type", liveIdInteger % 2 == 0? "shootyell":"showTime");
//      toInsert.put("template_id", random.nextInt(20000) % (20000) + 500111);
//      toInsert.put("from", "lecture");
//      toInsert.put("role", "lecture");
//      toInsert.put("body", "{小丫么小儿郎。。。。。。。。。。。。。。。。。}");
//      toInsert.put("state", "START");
//      toInsert.put("create_time", System.currentTimeMillis());
//      toInsert.put("update_time", System.currentTimeMillis());
//      bulkInserts.add(toInsert);
//      collection.insertMany(bulkInserts, INSERT_UNORDERED);
//      bulkInserts.clear();
//      return Status.OK;
//    } catch (Exception e) {
//      System.err.println("Exception while trying bulk insert with "
//          + bulkInserts.size());
//      e.printStackTrace();
//      return Status.ERROR;
//    }
//  }

  @Override
  public Status insert(String table, String key,
                       Map<String, ByteIterator> values) {
    try {
      MongoCollection<Document> collection = database.getCollection(table);
      int liveIdInteger = liveId.incrementAndGet();
//      for(int i = 0; i < 100; i++){
      Random random = new Random();
      Document toInsert = new Document("_id", UUID.randomUUID().toString());
      toInsert.put("live_id", random.nextInt(1000000) + 1);
      toInsert.put("lecture_id", random.nextInt(1000000) + 2000001);
      //500-20000的随机数
      int total = random.nextInt(20000) + 500;
      toInsert.put("interaction_id", total);
      toInsert.put("interaction_type", "xiaoming"+ total);
      toInsert.put("status", "create");
      toInsert.put("live_start_timne", 1587135000000L + random.nextInt(20000));
      toInsert.put("start_time", 1587135200000L + random.nextInt(20000));
      toInsert.put("end_time", 1587135220000L + random.nextInt(20000));
      toInsert.put("finish_time", 1587135240000L + random.nextInt(20000));
      toInsert.put("create_time", System.currentTimeMillis());
      toInsert.put("update_time", System.currentTimeMillis());
      bulkInserts.add(toInsert);
//      }
      collection.insertMany(bulkInserts, INSERT_UNORDERED);
      bulkInserts.clear();
      return Status.OK;
    } catch (Exception e) {
      System.err.println("Exception while trying bulk insert with "
          + bulkInserts.size());
      e.printStackTrace();
      return Status.ERROR;
    }
  }

//  @Override
//  public Status insert(String table, String key,
//                       Map<String, ByteIterator> values) {
//    try {
//      MongoCollection<Document> collection = database.getCollection(table);
//
//      int liveIdInteger = liveId.incrementAndGet();
//      Random random = new Random();
//      for(int i = 0; i < 10; i++){
//        for(int j = 0; j < 10; j++){
//          Document toInsert = new Document("_id", UUID.randomUUID().toString());
//          toInsert.put("live_id", liveIdInteger);
//          toInsert.put("tutor_id", j + 1000001);
//          toInsert.put("class_id", "");
//          toInsert.put("stu_id", j + 2000001);
//
//          toInsert.put("interaction_id", i + 5000);
//          toInsert.put("live_mode", "{\"live-mode\":\"sit\"}");
//
//          //500-20000的随机数
//          int total = random.nextInt(20000) % (20000) + 500;
//          toInsert.put("template", total);
//          toInsert.put("interaction_type", "sssss");
//          toInsert.put("interaction_version", 0);
//          toInsert.put("body", "{小丫么小儿郎。。。。。。。。。。。。。。。。。}");
//          toInsert.put("create_time", System.currentTimeMillis());
//          toInsert.put("update_time", System.currentTimeMillis());
//          bulkInserts.add(toInsert);
//        }
//      }
//      collection.insertMany(bulkInserts, INSERT_UNORDERED);
//      bulkInserts.clear();
//      return Status.OK;
//    } catch (Exception e) {
//      System.err.println("Exception while trying bulk insert with "
//          + bulkInserts.size());
//      e.printStackTrace();
//      return Status.ERROR;
//    }
//  }
//
//  @Override
//  public Status insert(String table, String key,
//                       Map<String, ByteIterator> values) {
//    try {
//      MongoCollection<Document> collection = database.getCollection(table);
//
//      int liveIdInteger = liveId.incrementAndGet();
//      Random random = new Random();
//      for(int tutorId = 0; tutorId < 100; tutorId++){
//        Document toInsert = new Document("_id", UUID.randomUUID().toString());
//        toInsert.put("live_id", liveIdInteger);
//        toInsert.put("tutor_id", tutorId);
//        toInsert.put("cause", "辅导巡场内禁言");
//        toInsert.put("img_url", "www.baidu.com");
//        toInsert.put("type", random.nextInt(2) + 1);
//        toInsert.put("report_id", 123);
//        toInsert.put("create_time", System.currentTimeMillis());
//        toInsert.put("status", 1);
//        toInsert.put("stu_id", tutorId + 2000001);
//        bulkInserts.add(toInsert);
//      }
//      collection.insertMany(bulkInserts, INSERT_UNORDERED);
//      bulkInserts.clear();
//      return Status.OK;
//    } catch (Exception e) {
//      System.err.println("Exception while trying bulk insert with "
//          + bulkInserts.size());
//      e.printStackTrace();
//      return Status.ERROR;
//    }
//  }

//  @Override
//  public Status insert(String table, String key,
//                       Map<String, ByteIterator> values) {
//    try {
//      MongoCollection<Document> collection = database.getCollection(table);
//
//      int liveIdInteger = liveId.incrementAndGet();
//      Random random = new Random();
//      int j = random.nextInt(10) + 1;
//      Document toInsert = new Document("_id", UUID.randomUUID().toString());
//      toInsert.put("live_id", liveIdInteger);
//      toInsert.put("tutor_id", random.nextInt(1000) +  1000001);
//      toInsert.put("big_group_id", 1);
//      toInsert.put("group_id", j);
//      toInsert.put("group_name", "斑马组"+j);
//      toInsert.put("group_motto", "我们齐心协力，一定能取得第一名");
//      //500-20000的随机数
//      toInsert.put("team_flag", j);
//      toInsert.put("room_id", j);
//      toInsert.put("token", "c3e1852ce11932ea1a8cb972d2be33c5");
//      toInsert.put("group_type", 0);
//      toInsert.put("full_pinyin", "banmazu"+j);
//      toInsert.put("simple_pinyin", "bmz");
//      toInsert.put("create_time", System.currentTimeMillis());
//      toInsert.put("update_time", System.currentTimeMillis());
//      bulkInserts.add(toInsert);
//
//      collection.insertMany(bulkInserts, INSERT_UNORDERED);
//      bulkInserts.clear();
//      return Status.OK;
//    } catch (Exception e) {
//      System.err.println("Exception while trying bulk insert with "
//          + bulkInserts.size());
//      e.printStackTrace();
//      return Status.ERROR;
//    }
//  }

//  @Override
//  public Status insert(String table, String key,
//                       Map<String, ByteIterator> values) {
//    try {
//      MongoCollection<Document> collection = database.getCollection(table);
//
//      int liveIdInteger = liveId.incrementAndGet();
//      Random random = new Random();
//      Document toInsert = new Document("_id", UUID.randomUUID().toString());
//      toInsert.put("live_id", liveIdInteger);
//      toInsert.put("tutor_id", random.nextInt(1000) +  1000001);
//      toInsert.put("stu_id", random.nextInt(1000) +  1000001);
//      toInsert.put("arrive_time",  System.currentTimeMillis());
//      toInsert.put("arrive_status", "attend");
//      toInsert.put("terminal", "pad");
//      toInsert.put("create_time", System.currentTimeMillis());
//      toInsert.put("update_time", System.currentTimeMillis());
//      bulkInserts.add(toInsert);
//
//      collection.insertMany(bulkInserts, INSERT_UNORDERED);
//      bulkInserts.clear();
//      return Status.OK;
//    } catch (Exception e) {
//      System.err.println("Exception while trying bulk insert with "
//          + bulkInserts.size());
//      e.printStackTrace();
//      return Status.ERROR;
//    }
//  }

//  @Override
//  public Status insert(String table, String key,
//                       Map<String, ByteIterator> values) {
//    try {
//      MongoCollection<Document> collection = database.getCollection(table);
//
//      int liveIdInteger = liveId.incrementAndGet();
//      Random random = new Random();
//      Document toInsert = new Document("_id", UUID.randomUUID().toString());
//      toInsert.put("live_id", liveIdInteger);
//      toInsert.put("tutor_id", random.nextInt(1000) + 1000001);
//      toInsert.put("big_group_id", 1);
//      toInsert.put("group_id", random.nextInt(1000) + 1);
//      toInsert.put("stu_id", random.nextInt(1000) + 2000001);
//      toInsert.put("status",  200);
//      toInsert.put("user_type", 0);
//      toInsert.put("target", 0);
//      toInsert.put("create_time", System.currentTimeMillis());
//      toInsert.put("update_time", System.currentTimeMillis());
//      bulkInserts.add(toInsert);
//      collection.insertMany(bulkInserts, INSERT_UNORDERED);
//      bulkInserts.clear();
//      return Status.OK;
//    } catch (Exception e) {
//      System.err.println("Exception while trying bulk insert with "
//          + bulkInserts.size());
//      e.printStackTrace();
//      return Status.ERROR;
//    }
//  }
  /**
   * Read a record from the database. Each field/value pair from the result will
   * be stored in a HashMap.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to read.
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error or "not found".
   */
  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    try {
      MongoCollection<Document> collection = database.getCollection(table);

      int biglive = (int)(Math.random()*(500000)+ 10000001);
      Random random = new Random();
      Document query = new Document();

      query.put("live_id", biglive);
      query.put("tutor_id", random.nextInt(100) + 2000001);
      FindIterable<Document> findIterable = collection.find(query);
      Document queryResult = findIterable.first();
      if (queryResult != null) {
        fillMap(result, queryResult);
      }
      return queryResult != null ? Status.OK : Status.NOT_FOUND;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    }
  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value
   * pair from the result will be stored in a HashMap.
   *
   * @param table
   *          The name of the table
   * @param startkey
   *          The record key of the first record to read.
   * @param recordcount
   *          The number of records to read
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A Vector of HashMaps, where each HashMap is a set field/value
   *          pairs for one record
   * @return Zero on success, a non-zero error code on error. See the {@link DB}
   *         class's description for a discussion of error codes.
   */
  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    MongoCursor<Document> cursor = null;
    try {
      MongoCollection<Document> collection = database.getCollection(table);

      Document scanRange = new Document("$gte", startkey);
      Document query = new Document("_id", scanRange);
      Document sort = new Document("_id", INCLUDE);

      FindIterable<Document> findIterable =
          collection.find(query).sort(sort).limit(recordcount);

      if (fields != null) {
        Document projection = new Document();
        for (String fieldName : fields) {
          projection.put(fieldName, INCLUDE);
        }
        findIterable.projection(projection);
      }

      cursor = findIterable.iterator();

      if (!cursor.hasNext()) {
        System.err.println("Nothing found in scan for key " + startkey);
        return Status.ERROR;
      }

      result.ensureCapacity(recordcount);

      while (cursor.hasNext()) {
        HashMap<String, ByteIterator> resultMap =
            new HashMap<String, ByteIterator>();

        Document obj = cursor.next();
        fillMap(resultMap, obj);

        result.add(resultMap);
      }

      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    } finally {
      if (cursor != null) {
        cursor.close();
      }
    }
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key, overwriting any existing values with the same field name.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to write.
   * @param values
   *          A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error. See this class's
   *         description for a discussion of error codes.
   */
  @Override
  public Status update(String table, String key,
                       Map<String, ByteIterator> values) {
    try {
      MongoCollection<Document> collection = database.getCollection(table);

      Document query = new Document("_id", key);
      Document fieldsToSet = new Document();
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        fieldsToSet.put(entry.getKey(), entry.getValue().toArray());
      }
      Document update = new Document("$set", fieldsToSet);

      UpdateResult result = collection.updateOne(query, update);
      if (result.wasAcknowledged() && result.getMatchedCount() == 0) {
        System.err.println("Nothing updated for key " + key);
        return Status.NOT_FOUND;
      }
      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    }
  }

  /**
   * Fills the map with the values from the DBObject.
   *
   * @param resultMap
   *          The map to fill/
   * @param obj
   *          The object to copy values from.
   */
  protected void fillMap(Map<String, ByteIterator> resultMap, Document obj) {
    for (Map.Entry<String, Object> entry : obj.entrySet()) {
      if (entry.getValue() instanceof Binary) {
        resultMap.put(entry.getKey(),
            new ByteArrayByteIterator(((Binary) entry.getValue()).getData()));
      }
    }
  }
}
