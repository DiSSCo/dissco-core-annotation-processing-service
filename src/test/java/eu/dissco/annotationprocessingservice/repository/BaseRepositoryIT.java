package eu.dissco.annotationprocessingservice.repository;

import static eu.dissco.annotationprocessingservice.TestUtils.CREATED;
import static eu.dissco.annotationprocessingservice.TestUtils.ID;
import static eu.dissco.annotationprocessingservice.TestUtils.TARGET_ID;
import static eu.dissco.annotationprocessingservice.database.jooq.Tables.MAS_JOB_RECORD;
import static org.testcontainers.containers.PostgreSQLContainer.IMAGE;

import com.zaxxer.hikari.HikariDataSource;
import eu.dissco.annotationprocessingservice.database.jooq.enums.JobState;
import eu.dissco.annotationprocessingservice.database.jooq.enums.MjrTargetType;
import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DefaultDSLContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public class BaseRepositoryIT {

  private static final DockerImageName POSTGIS =
      DockerImageName.parse("postgres:13.7").asCompatibleSubstituteFor(IMAGE);

  @Container
  private static final PostgreSQLContainer<?> CONTAINER = new PostgreSQLContainer<>(POSTGIS);
  protected DSLContext context;
  private HikariDataSource dataSource;

  @BeforeEach
  void prepareDatabase() {
    dataSource = new HikariDataSource();
    dataSource.setJdbcUrl(CONTAINER.getJdbcUrl());
    dataSource.setUsername(CONTAINER.getUsername());
    dataSource.setPassword(CONTAINER.getPassword());
    dataSource.setMaximumPoolSize(2);
    dataSource.setConnectionInitSql(CONTAINER.getTestQueryString());
    Flyway.configure().mixed(true).dataSource(dataSource).load().migrate();
    context = new DefaultDSLContext(dataSource, SQLDialect.POSTGRES);
  }

  @AfterEach
  void disposeDataSource() {
    dataSource.close();
  }

  protected void postMjr(String jobId) {
    context.insertInto(MAS_JOB_RECORD, MAS_JOB_RECORD.JOB_ID, MAS_JOB_RECORD.JOB_STATE,
            MAS_JOB_RECORD.MAS_ID, MAS_JOB_RECORD.TARGET_ID, MAS_JOB_RECORD.TARGET_TYPE,
            MAS_JOB_RECORD.TIME_STARTED, MAS_JOB_RECORD.BATCHING_REQUESTED)
        .values(jobId, JobState.SCHEDULED, ID, TARGET_ID, MjrTargetType.DIGITAL_SPECIMEN,
            CREATED, false)
        .execute();
  }

}
