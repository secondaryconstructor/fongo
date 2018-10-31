package com.mongodb;


import com.github.fakemongo.Fongo;
import com.github.fakemongo.junit.FongoRule;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.UUID;
import org.bson.BsonDocument;
import org.bson.Document;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;


/**
 * These tests check if a collection name is valid before being created by an insert command.
 * If it isn't valid an IllegalArgumentException will be thrown.
 * <p>
 * Reference: The _id field not returned for collections with name starting with "system" #242
 *
 * @author Nicola Viola
 */
public class CollectionNameTest {

  @Rule
  public final FongoRule fongoRule = new FongoRule(false);
  private UUID id;
  private Document document;
  private MongoClient mongo;

  @Before
  public void setUp() {
    id = UUID.randomUUID();
    document = new Document("_id", id);
    mongo = fongoRule.getMongoClient();
  }

  @Test
  public void collectionnameIsSystemUser_thenTheDocumentIsFound() {

    String collectionName = "systemUser";
    UUID result = findADocumentInACollection(collectionName, document, mongo);
    assertThat(result, is(id));
  }

  @Test(expected = MongoCommandException.class)
  public void collectionnameIsSystemDotUser_throwIllegalArgumentException() {
    String collectionName = "system.User";
    findADocumentInACollection(collectionName, document, mongo);
  }

  @Test(expected = MongoCommandException.class)
  public void collectionnameIsSystemDot_throwIllegalArgumentException() {
    String collectionName = "system.";

    findADocumentInACollection(collectionName, document, mongo);
  }

  @Test
  public void collectionnameIsSystem_thenTheDocumentIsFound() {
    String collectionName = "system";

    UUID result = findADocumentInACollection(collectionName, document, mongo);
    assertThat(result, is(id));
  }


  @Test(expected = MongoCommandException.class)
  public void collectionnameIs$t_thenTheDocumentIsFound() {
    String collectionName = "$t";

    findADocumentInACollection(collectionName, document, mongo);
  }


  private UUID findADocumentInACollection(String collectionName, Document document, MongoClient fongo) {

    MongoDatabase database = fongo.getDatabase("test-db");

    MongoCollection<Document> systemUsers = database.getCollection(collectionName);
    systemUsers.deleteMany(new BsonDocument());
    systemUsers.insertOne(document);
    final Document retrievedDocument = systemUsers.find().iterator().next();

    return retrievedDocument.get("_id", UUID.class);
  }


}