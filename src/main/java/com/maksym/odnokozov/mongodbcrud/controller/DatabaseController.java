package com.maksym.odnokozov.mongodbcrud.controller;

import com.maksym.odnokozov.mongodbcrud.model.Collection;
import com.maksym.odnokozov.mongodbcrud.model.Database;
import com.maksym.odnokozov.mongodbcrud.service.MongoDBService;
import org.bson.BsonDocument;
import org.bson.Document;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/databases")
public class DatabaseController {
    private final MongoDBService mongoDBService;

    public DatabaseController(MongoDBService mongoDBService) {
        this.mongoDBService = mongoDBService;
    }

    @GetMapping
    public List<Database> getDatabases() {
        return mongoDBService.getAllDatabases();
    }

    @GetMapping("/{databaseName}")
    public List<Collection> getCollections(@PathVariable String databaseName) {
        return mongoDBService.getDatabaseCollections(databaseName);
    }

    @GetMapping("/{database}/{collection}")
    public List<Document> showDocuments(@PathVariable String database,
                                        @PathVariable String collection) {
        return mongoDBService.getAllDocuments(database, collection);
    }

    @GetMapping("/{database}/{collection}/filter")
    public List<Document> filterDocuments(@PathVariable String database,
                                          @PathVariable String collection,
                                          @RequestBody String filterJson) {
        BsonDocument filter = BsonDocument.parse(filterJson);
        return mongoDBService.getFilteredDocuments(database, collection, filter);
    }

    @PostMapping("/{database}/{collection}")
    public ResponseEntity<String> addDocument(@PathVariable String database,
                                              @PathVariable String collection,
                                              @RequestBody Document document) {
        boolean success = mongoDBService.addDocument(database, collection, document);
        if (success) {
            return ResponseEntity.ok("Document added successfully");
        } else {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Failed to add document");
        }
    }

    @PostMapping("/{database}/{collection}/batch")
    public ResponseEntity<String> addDocumentsBatch(@PathVariable String database,
                                                    @PathVariable String collection,
                                                    @RequestBody List<Document> documents) {
        boolean success = mongoDBService.addDocuments(database, collection, documents);
        if (success) {
            return ResponseEntity.ok("Documents added successfully");
        } else {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Failed to add documents");
        }
    }

    @PutMapping("/{database}/{collection}/{documentId}")
    public ResponseEntity<String> updateDocument(@PathVariable String database,
                                                 @PathVariable String collection,
                                                 @PathVariable String documentId,
                                                 @RequestBody Document document) {
        boolean success = mongoDBService.updateDocument(database, collection, documentId, document);
        if (success) {
            return ResponseEntity.ok("Document updated successfully");
        } else {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Failed to update document");
        }
    }

    @PutMapping("/{database}/{collection}/batch")
    public ResponseEntity<String> updateDocumentsBatch(@PathVariable String database,
                                                       @PathVariable String collection,
                                                       @RequestBody List<Document> documents) {
        boolean success = mongoDBService.updateDocuments(database, collection, documents);

        if (success) {
            return ResponseEntity.ok("Documents updated successfully");
        } else {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Failed to update documents");
        }
    }

    @DeleteMapping("/{database}/{collection}/{documentId}")
    public ResponseEntity<String> deleteDocument(@PathVariable String database, @PathVariable String collection,
                                                 @PathVariable String documentId) {
        boolean success = mongoDBService.deleteDocument(database, collection, documentId);
        if (success) {
            return ResponseEntity.ok("Document deleted successfully");
        } else {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Failed to delete document");
        }
    }

    @DeleteMapping("/{database}/{collection}/batch")
    public ResponseEntity<String> deleteDocumentsBatch(@PathVariable String database, @PathVariable String collection,
                                                       @RequestBody List<String> documentIds) {
        boolean success = mongoDBService.deleteDocuments(database, collection, documentIds);
        if (success) {
            return ResponseEntity.ok("Documents deleted successfully");
        } else {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Failed to delete documents");
        }
    }


}


