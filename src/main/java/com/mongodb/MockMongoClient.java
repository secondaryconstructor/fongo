package com.mongodb;

import com.github.fakemongo.Fongo;
import com.github.fakemongo.FongoConnection;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.client.MongoDatabase;
import com.mongodb.connection.AsyncConnection;
import com.mongodb.connection.BufferProvider;
import com.mongodb.connection.Cluster;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.Connection;
import com.mongodb.connection.Server;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.connection.PowerOfTwoBufferPool;
import com.mongodb.operation.OperationExecutor;
import com.mongodb.selector.ServerSelector;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.objenesis.ObjenesisStd;

public class MockMongoClient extends MongoClient {

  // this is immutable 
  private final static MongoClientOptions clientOptions = MongoClientOptions.builder().build();

  private volatile BufferProvider bufferProvider;

  private Fongo fongo;
  private MongoOptions options;
  private ReadConcern readConcern;

  public static MockMongoClient create(Fongo fongo) {
    // using objenesis here to prevent default constructor from spinning up background threads.
    MockMongoClient client = new ObjenesisStd().getInstantiatorOf(MockMongoClient.class).newInstance();
    client.options = new MongoOptions(clientOptions);
    client.fongo = fongo;
    client.setWriteConcern(clientOptions.getWriteConcern());
    client.setReadPreference(clientOptions.getReadPreference());
    client.readConcern = clientOptions.getReadConcern() == null ? ReadConcern.DEFAULT : clientOptions.getReadConcern();
    try {
      Mongo.class.getDeclaredMethod("getClusterDescription").setAccessible(true);
      Mongo.class.getDeclaredMethod("getClusterDescription").
      Mongo.class.getDeclaredField("cluster").setAccessible(true);
      Mongo.class.getDeclaredField("cluster").set(client, client.getCluster());
    } catch (IllegalAccessException e) {      e.printStackTrace();

      e.printStackTrace();
    } catch (NoSuchFieldException e) {
    } catch (NoSuchMethodException e) {
      e.printStackTrace();
    }
    return client;
  }

  public MockMongoClient() throws UnknownHostException {
  }

  @Override
  public String toString() {
    return fongo.toString();
  }

  @Override
  public Collection<DB> getUsedDatabases() {
    return fongo.getUsedDatabases();
  }

  @Override
  public List<String> getDatabaseNames() {
    return fongo.getDatabaseNames();
  }

  @Override
  public int getMaxBsonObjectSize() {
    return 16 * 1024 * 1024;
  }

  @Override
  public DB getDB(String dbname) {
    return this.fongo.getDB(dbname);
  }

  @Override
  public MongoDatabase getDatabase(final String databaseName) {
    return new FongoMongoDatabase(databaseName, this.fongo);
  }

  @Override
  public void dropDatabase(String dbName) {
    this.fongo.dropDatabase(dbName);
  }

  @Override
  public MongoOptions getMongoOptions() {
    return this.options;
  }

  @Override
  public MongoClientOptions getMongoClientOptions() {
    return clientOptions;
  }

  @Override
  public List<ServerAddress> getAllAddress() {
    return super.getAllAddress();
  }

  @Override
  public List<ServerAddress> getServerAddressList() {
    return Collections.singletonList(fongo.getServerAddress());
  }

  @Override
  public Cluster getCluster() {
    return new Cluster() {
      @Override
      public ClusterDescription getDescription() {
        return null;
      }

      @Override
      public Server selectServer(ServerSelector serverSelector) {
        return new Server() {
          @Override
          public ServerDescription getDescription() {
            return new ObjenesisStd().getInstantiatorOf(ServerDescription.class).newInstance();
          }

          @Override
          public Connection getConnection() {
            return new FongoConnection(fongo);
          }

          @Override
          public void getConnectionAsync(SingleResultCallback<AsyncConnection> callback) {
            // TODO
          }

        };
      }

      @Override
      public void selectServerAsync(ServerSelector serverSelector, SingleResultCallback<Server> callback) {

      }

      @Override
      public void close() {

      }

      @Override
      public boolean isClosed() {
        return false;
      }
    };
  }

  OperationExecutor createOperationExecutor() {
    return fongo;
  }

  @Override
  public void close() {
  }

  @Override
  synchronized BufferProvider getBufferProvider() {
    if (bufferProvider == null) {
      bufferProvider = new PowerOfTwoBufferPool();
    }
    return bufferProvider;
  }

  @Override
  public ReadConcern getReadConcern() {
    return readConcern;
  }
}
