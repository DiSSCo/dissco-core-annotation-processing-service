package eu.dissco.annotationprocessingservice.repository;

import static eu.dissco.annotationprocessingservice.TestUtils.CREATED;
import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.database.jooq.Tables.HANDLES;
import static org.assertj.core.api.Assertions.assertThat;

import eu.dissco.annotationprocessingservice.domain.HandleAttribute;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.jooq.Record1;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HandleRepositoryIT extends BaseRepositoryIT {

  private HandleRepository repository;

  @BeforeEach
  void setup() {
    repository = new HandleRepository(context);
  }

  @AfterEach
  void destroy() {
    context.truncate(HANDLES).execute();
  }

  @Test
  void testCreateHandle() {
    // Given
    var handleAttributes = givenHandleAttributes();

    // When
    repository.createHandle(ID, CREATED, handleAttributes);

    // Then
    var handles = context.selectFrom(HANDLES).fetch();
    assertThat(handles).hasSize(handleAttributes.size());
  }

  @Test
  void testUpdateHandleAttributes() {
    // Given
    var handleAttributes = givenHandleAttributes();
    repository.createHandle(ID, CREATED, handleAttributes);
    var updatedHandle = new HandleAttribute(11, "pidKernelMetadataLicense",
        "anotherLicenseType".getBytes(StandardCharsets.UTF_8));

    // When
    repository.updateHandleAttributes(ID, CREATED, List.of(updatedHandle), true);

    // Then
    var result = context.select(HANDLES.DATA)
        .from(HANDLES)
        .where(HANDLES.HANDLE.eq(ID.getBytes(StandardCharsets.UTF_8)))
        .and(HANDLES.TYPE.eq("issueNumber".getBytes(StandardCharsets.UTF_8)))
        .fetchOne(Record1::value1);
    assertThat(result).isEqualTo("2".getBytes(StandardCharsets.UTF_8));
  }

  @Test
  void testRollbackVersion() {
    // Given
    var handleAttributes = givenHandleAttributes();
    repository.createHandle(ID, CREATED, handleAttributes);
    var updatedHandle = new HandleAttribute(11, "pidKernelMetadataLicense",
        "anotherLicenseType".getBytes(StandardCharsets.UTF_8));
    repository.updateHandleAttributes(ID, CREATED, List.of(updatedHandle), true);
    // When

    repository.updateHandleAttributes(ID, CREATED, handleAttributes, false);

    // Then
    var result = context.select(HANDLES.DATA)
        .from(HANDLES)
        .where(HANDLES.HANDLE.eq(ID.getBytes(StandardCharsets.UTF_8)))
        .and(HANDLES.TYPE.eq("issueNumber".getBytes(StandardCharsets.UTF_8)))
        .fetchOne(Record1::value1);
    assertThat(result).isEqualTo("1".getBytes(StandardCharsets.UTF_8));
  }

  @Test
  void testRollbackHandle() {
    // Given
    var handleAttributes = givenHandleAttributes();
    repository.createHandle(ID, CREATED, handleAttributes);

    // When
    repository.rollbackHandleCreation(ID);

    // Then
    var handles = context.selectFrom(HANDLES).fetch();
    assertThat(handles).isEmpty();
  }

  @Test
  void testArchiveHandle() {
    // Given
    var status = new HandleAttribute(8, "pidStatus", "ARCHIVED".getBytes(StandardCharsets.UTF_8));
    var text = new HandleAttribute(9, "tombstoneText",
        ("Annotation owner: XXX has archived annotation").getBytes(StandardCharsets.UTF_8));
    var handleAttributes = givenHandleAttributes();
    repository.createHandle(ID, CREATED, handleAttributes);

    // When
    repository.archiveAnnotation(ID, CREATED, status, text);

    // Then
    var handles = context.selectFrom(HANDLES)
        .where(HANDLES.HANDLE.eq(ID.getBytes(StandardCharsets.UTF_8)))
        .and(HANDLES.TYPE.eq("pidStatus".getBytes(StandardCharsets.UTF_8))).fetchOne();
    assertThat(handles.get(HANDLES.DATA)).isEqualTo("ARCHIVED".getBytes(StandardCharsets.UTF_8));
  }

  private List<HandleAttribute> givenHandleAttributes() {
    return List.of(
        new HandleAttribute(1, "pid",
            ("https://hdl.handle.net/" + ID).getBytes(StandardCharsets.UTF_8)),
        new HandleAttribute(11, "pidKernelMetadataLicense",
            "https://creativecommons.org/publicdomain/zero/1.0/".getBytes(StandardCharsets.UTF_8)),
        new HandleAttribute(7, "issueNumber", "1".getBytes(StandardCharsets.UTF_8)),
        new HandleAttribute(8, "pidStatus", "ACTIVE".getBytes(StandardCharsets.UTF_8)),
        new HandleAttribute(100, "HS_ADMIN", "TEST_ADMIN_STRING".getBytes(StandardCharsets.UTF_8))
    );
  }

}
