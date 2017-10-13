package com.mongodb;

public class InsertManyWriteConcernException extends WriteConcernException {

  private BulkWriteResult bulkWriteResult;
  private int erroneousIndex;

  public InsertManyWriteConcernException(BulkWriteResult bulkWriteResult, int erroneousIndex, WriteConcernException e) {
    super(e.getResponse(), e.getServerAddress(), e.getWriteConcernResult());

    this.bulkWriteResult = bulkWriteResult;
    this.erroneousIndex = erroneousIndex;
  }

  public BulkWriteResult getBulkWriteResult() {
    return bulkWriteResult;
  }

  public int getErroneousIndex() {
    return erroneousIndex;
  }

}
