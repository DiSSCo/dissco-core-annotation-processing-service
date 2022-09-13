package eu.dissco.annotationprocessingservice.repository;

import static eu.dissco.annotationprocessingservice.database.jooq.Tables.HANDLES;

import eu.dissco.annotationprocessingservice.domain.HandleAttribute;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.Query;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class HandleRepository {

  private final DSLContext context;

  public void createHandle(String handle, Instant recordTimestamp,
      List<HandleAttribute> handleAttributes) {
    var queryList = new ArrayList<Query>();
    for (var handleAttribute : handleAttributes) {
      var query = context.insertInto(HANDLES)
          .set(HANDLES.HANDLE, handle.getBytes(StandardCharsets.UTF_8))
          .set(HANDLES.IDX, handleAttribute.index())
          .set(HANDLES.TYPE, handleAttribute.type().getBytes(StandardCharsets.UTF_8))
          .set(HANDLES.DATA, handleAttribute.data())
          .set(HANDLES.TTL, 86400)
          .set(HANDLES.TIMESTAMP, recordTimestamp.getEpochSecond())
          .set(HANDLES.ADMIN_READ, true)
          .set(HANDLES.ADMIN_WRITE, true)
          .set(HANDLES.PUB_READ, true)
          .set(HANDLES.PUB_WRITE, false);
      queryList.add(query);
    }
    context.batch(queryList).execute();
  }

  public void updateHandleAttributes(String id, Instant recordTimestamp,
      List<HandleAttribute> handleAttributes) {
    var queryList = new ArrayList<Query>();
    for (var handleAttribute : handleAttributes) {
      var query = context.update(HANDLES)
          .set(HANDLES.DATA, handleAttribute.data())
          .set(HANDLES.TIMESTAMP, recordTimestamp.getEpochSecond())
          .where(HANDLES.HANDLE.eq(id.getBytes(StandardCharsets.UTF_8)))
          .and(HANDLES.IDX.eq(handleAttribute.index()));
      queryList.add(query);
    }
    queryList.addAll(versionIncrement(id, recordTimestamp));
    context.batch(queryList).execute();
  }

  private List<Query> versionIncrement(String pid, Instant recordTimestamp) {
    var issueAttributes = new ArrayList<Query>();
    var currentVersion =
        Integer.parseInt(context.select(HANDLES.DATA)
            .from(HANDLES)
            .where(HANDLES.HANDLE.eq(pid.getBytes(
                StandardCharsets.UTF_8)))
            .and(HANDLES.TYPE.eq("issueNumber".getBytes(StandardCharsets.UTF_8)))
            .fetchOne(dbRecord -> new String(dbRecord.value1())));
    var version = currentVersion + 1;
    issueAttributes.add(context.update(HANDLES)
        .set(HANDLES.DATA, String.valueOf(version).getBytes(StandardCharsets.UTF_8))
        .set(HANDLES.TIMESTAMP, recordTimestamp.getEpochSecond())
        .where(HANDLES.HANDLE.eq(pid.getBytes(
            StandardCharsets.UTF_8)))
        .and(HANDLES.TYPE.eq("issueNumber".getBytes(StandardCharsets.UTF_8))));
    issueAttributes.add(context.update(HANDLES)
        .set(HANDLES.DATA, createIssueDate(recordTimestamp))
        .set(HANDLES.TIMESTAMP, recordTimestamp.getEpochSecond())
        .where(HANDLES.HANDLE.eq(pid.getBytes(
            StandardCharsets.UTF_8)))
        .and(HANDLES.TYPE.eq("issueDate".getBytes(StandardCharsets.UTF_8))));
    return issueAttributes;
  }

  public void archiveAnnotation(String id, Instant recordTimestamp, HandleAttribute status, HandleAttribute text) {
    var queryList = new ArrayList<Query>();
    var statusQuery = context.update(HANDLES)
        .set(HANDLES.DATA, status.data())
        .set(HANDLES.TIMESTAMP, recordTimestamp.getEpochSecond())
        .where(HANDLES.HANDLE.eq(id.getBytes(StandardCharsets.UTF_8)))
        .and(HANDLES.IDX.eq(status.index()));
    queryList.add(statusQuery);
    var textQuery = context.insertInto(HANDLES)
        .set(HANDLES.HANDLE, id.getBytes(StandardCharsets.UTF_8))
        .set(HANDLES.IDX, text.index())
        .set(HANDLES.TYPE, text.type().getBytes(StandardCharsets.UTF_8))
        .set(HANDLES.DATA, text.data())
        .set(HANDLES.TTL, 86400)
        .set(HANDLES.TIMESTAMP, recordTimestamp.getEpochSecond())
        .set(HANDLES.ADMIN_READ, true)
        .set(HANDLES.ADMIN_WRITE, true)
        .set(HANDLES.PUB_READ, true)
        .set(HANDLES.PUB_WRITE, false);
    queryList.add(textQuery);
    queryList.addAll(versionIncrement(id, recordTimestamp));
    context.batch(queryList).execute();
  }

  private byte[] createIssueDate(Instant recordTimestamp) {
    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
    return formatter.format(Date.from(recordTimestamp)).getBytes(StandardCharsets.UTF_8);
  }
}
