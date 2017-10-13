package com.github.fakemongo;

import java.util.ArrayList;
import java.util.List;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import com.github.fakemongo.junit.FongoRule;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.client.MongoCollection;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class FongoInsertManyTest {

  public static final boolean REAL_MONGO = false;

  @Rule
  public FongoRule fongoRule = new FongoRule(REAL_MONGO);

  @After
  public void tearDown() throws Exception {
    fongoRule.getDatabase("db").getCollection("collection").drop();
  }

  @Test
  public void should_insert_all_given_documents() throws Exception {
    // Given
    MongoCollection<Document> collection = fongoRule.getDatabase("db").getCollection("collection");
    Document document1 = new Document("_id", 1);
    Document document2 = new Document("_id", 2);
    Document document3 = new Document("_id", 3);

    // when
    collection.insertMany(asList(document1, document2, document3));

    // then
    final List<Document> documents = collection.find().into(new ArrayList<Document>());

    // Then
    assertThat(documents).containsExactly(document1, document2, document3);
  }

  @Test
  public void should_throw_MongoBulkWriteException_on_first_insert_and_abort_consequent_inserts() {
    // Given
    MongoCollection<Document> collection = fongoRule.getDatabase("db").getCollection("collection");
    Document document1 = new Document("_id", 1);
    Document document2 = new Document("_id", 2);
    Document document3 = new Document("_id", 3);
    collection.insertOne(document1);

    // When/Then
    try {
      collection.insertMany(asList(document1, document2, document3));
    } catch (MongoBulkWriteException ex) {
      // then
      assertThat(ex.getWriteErrors()).containsExactly(
          new BulkWriteError(11000, getDuplicateKeyMessage(1), new BsonDocument(), 0)
      );
      assertThat(ex.getWriteResult().getInsertedCount()).isEqualTo(0);
    }
    final List<Document> documents = collection.find().into(new ArrayList<Document>());

    assertThat(documents).containsExactly(document1);
  }

  @Test
  public void should_throw_MongoBulkWriteException_on_last_insert_and_have_previous_successful_inserts() {
    // Given
    MongoCollection<Document> collection = fongoRule.getDatabase("db").getCollection("collection");
    Document document1 = new Document("_id", 1);
    Document document2 = new Document("_id", 2);
    Document document3 = new Document("_id", 3);
    collection.insertOne(document3);

    // When/Then
    try {
      collection.insertMany(asList(document1, document2, document3));
    } catch (MongoBulkWriteException ex) {
      // then
      assertThat(ex.getWriteErrors()).containsExactly(
          new BulkWriteError(11000, getDuplicateKeyMessage(3), new BsonDocument(), 2)
      );
      assertThat(ex.getWriteResult().getInsertedCount()).isEqualTo(2);
    }
    final List<Document> documents = collection.find().into(new ArrayList<Document>());

    assertThat(documents).containsExactly(document3, document1, document2);
  }

  private String getDuplicateKeyMessage(Object value) {
    if (REAL_MONGO) {
      return "E11000 duplicate key error index: db.collection.$_id_ dup key: { : " + value + " }";
    } else {
      return "E11000 duplicate key error index: db.collection._id  dup key : {[[" + value + "]] }";
    }
  }

}
