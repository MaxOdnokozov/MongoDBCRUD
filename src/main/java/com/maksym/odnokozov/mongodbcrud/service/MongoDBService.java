package com.maksym.odnokozov.mongodbcrud.service;

import com.maksym.odnokozov.mongodbcrud.model.Collection;
import com.maksym.odnokozov.mongodbcrud.model.Database;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
@AllArgsConstructor
public class MongoDBService {

    public static final String URI = "mongodb://localhost:27017";

    public List<Database> getAllDatabases() {
        List<Database> databases = new ArrayList<>();
        try (var mongoClient = MongoClients.create(URI)) {
            var databaseNames = mongoClient.listDatabaseNames();
            databaseNames.forEach(name -> databases.add(new Database(name)));
        }
        return databases;
    }

    public List<Collection> getDatabaseCollections(String databaseName) {
        List<Collection> collections = new ArrayList<>();
        try (var mongoClient = MongoClients.create(URI)) {
            var database = mongoClient.getDatabase(databaseName);
            var collectionNames = database.listCollectionNames();
            collectionNames.forEach(collectionName -> collections.add(new Collection(collectionName)));
        }
        return collections;
    }

    public List<Document> getAllDocuments(String databaseName, String collectionName) {
        List<Document> documentList;
        try (var mongoClient = MongoClients.create(URI)) {
            MongoDatabase database = mongoClient.getDatabase(databaseName);
            MongoCollection<Document> collection = database.getCollection(collectionName);
            FindIterable<Document> documents = collection.find();
            documentList = new ArrayList<>();
            for (Document document : documents) {
                documentList.add(document);
            }
        }
        log.info("Documents: {}", documentList);
        return documentList;
    }

    public List<Document> getFilteredDocuments(String databaseName, String collectionName, Bson filter) {
        List<Document> documentList;
        try (var mongoClient = MongoClients.create(URI)) {
            MongoDatabase database = mongoClient.getDatabase(databaseName);
            MongoCollection<Document> collection = database.getCollection(collectionName);

            FindIterable<Document> documents = collection.find(filter);

            documentList = new ArrayList<>();
            for (Document document : documents) {
                documentList.add(document);
            }
        }
        return documentList;
    }

    public boolean addDocument(String databaseName, String collectionName, Document document) {
        try (var mongoClient = MongoClients.create(URI)) {
            MongoDatabase database = mongoClient.getDatabase(databaseName);
            MongoCollection<Document> collection = database.getCollection(collectionName);
            collection.insertOne(document);
            return true;
        } catch (Exception e) {
            log.error("Exception during adding document, error message: {}", e.getMessage());
            return false;
        }
    }

    public boolean addDocuments(String databaseName, String collectionName, List<Document> documents) {
        try (var mongoClient = MongoClients.create(URI)) {
            MongoDatabase database = mongoClient.getDatabase(databaseName);
            MongoCollection<Document> collection = database.getCollection(collectionName);
            collection.insertMany(documents);
            return true;
        } catch (Exception e) {
            log.error("Exception during adding documents batch, error message: {}", e.getMessage());
            return false;
        }
    }

    public boolean updateDocument(String databaseName, String collectionName, String documentId, Document document) {
        try (var mongoClient = MongoClients.create(URI)) {
            MongoDatabase database = mongoClient.getDatabase(databaseName);
            MongoCollection<Document> collection = database.getCollection(collectionName);

            Bson filter = Filters.eq("_id", new ObjectId(documentId));

            UpdateResult result = collection.replaceOne(filter, document);
            return result.getModifiedCount() > 0;
        } catch (Exception e) {
            log.error("Exception during updating document, error message: {}", e.getMessage());;
            return false;
        }
    }

    public boolean updateDocuments(String databaseName, String collectionName, List<Document> documents) {
        try (var mongoClient = MongoClients.create(URI)) {
            MongoDatabase database = mongoClient.getDatabase(databaseName);
            MongoCollection<Document> collection = database.getCollection(collectionName);

            for (Document document : documents) {
                Bson filter = Filters.eq("_id", document.get("_id"));
                collection.replaceOne(filter, document);
            }
            return true;
        } catch (Exception e) {
            log.error("Exception during updating documents batch, error message: {}", e.getMessage());
            return false;
        }
    }

    public boolean deleteDocument(String databaseName, String collectionName, String documentId) {
        try (var mongoClient = MongoClients.create(URI)) {
            MongoDatabase database = mongoClient.getDatabase(databaseName);
            MongoCollection<Document> collection = database.getCollection(collectionName);

            Bson filter = Filters.eq("_id", new ObjectId(documentId));

            DeleteResult result = collection.deleteOne(filter);
            return result.getDeletedCount() > 0;
        } catch (Exception e) {
            log.error("Exception during deleting document, error message: {}", e.getMessage());
            return false;
        }
    }

    public boolean deleteDocuments(String databaseName, String collectionName, List<String> documentIds) {
        try (var mongoClient = MongoClients.create(URI)) {
            MongoDatabase database = mongoClient.getDatabase(databaseName);
            MongoCollection<Document> collection = database.getCollection(collectionName);

            List<Bson> filters = new ArrayList<>();
            for (String documentId : documentIds) {
                Bson filter = Filters.eq("_id", new ObjectId(documentId));
                filters.add(filter);
            }

            Bson deleteFilter = Filters.or(filters.toArray(new Bson[0]));
            DeleteResult result = collection.deleteMany(deleteFilter);

            return result.getDeletedCount() > 0;
        } catch (Exception e) {
            log.error("Exception during deleting document batch, error message: {}", e.getMessage());
            return false;
        }
    }
}
