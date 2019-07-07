package pl.edu.agh.csg;

import org.apache.commons.lang3.NotImplementedException;
import org.cloudbus.cloudsim.cloudlets.Cloudlet;
import org.cloudbus.cloudsim.cloudlets.CloudletSimple;
import org.cloudbus.cloudsim.util.DataCloudTags;
//import org.cloudbus.cloudsim.util.WorkloadReader;
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelFull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import static pl.edu.agh.csg.Defaults.withDefault;

public class DatabaseJobReader {//} implements WorkloadReader {
    private final String uri;
    private final Long endTime;
    private final double speedUp;
    private final Long startTime;

    private static final Logger logger = LoggerFactory.getLogger(DatabaseJobReader.class.getName());
    private final String dbUser;
    private final String dbPassword;
    private final int jobPeSplitThreshold;

    public DatabaseJobReader(Long startTime, Long endTime, int jobPeSplitThreshold) {
        this(startTime, endTime, jobPeSplitThreshold, 1.0);
    }

    public DatabaseJobReader(Long startTime, Long endTime, int jobPeSplitThreshold, double speedUp) {
        this.jobPeSplitThreshold = jobPeSplitThreshold;
        this.startTime = startTime;
        this.endTime = endTime;
        this.speedUp = speedUp;

        String dbHostname = withDefault("DATABASE_HOSTNAME", "localhost");
        int dbPort = Integer.valueOf(withDefault("DATABASE_PORT", "5432"));
        String dbName = withDefault("DATABASE_NAME", "samm_db");
        this.dbUser = withDefault("DATABASE_USER", "samm");
        this.dbPassword = withDefault("DATABASE_PASSWORD", "samm_secret");
        this.uri = String.format("jdbc:postgresql://%s:%d/%s", dbHostname, dbPort, dbName);
    }

//    @Override
    public List<Cloudlet> generateWorkload() throws IOException {

        List<Cloudlet> cloudlets = new ArrayList<>();
        Connection conn = null;
        Exception caught = null;
        try {
            conn = DriverManager.getConnection(uri, dbUser, dbPassword);

            PreparedStatement statement = conn.prepareStatement(
                    "SELECT " +
                            "job_id," +
                            "entry_timestamp, " +
                            "start_timestamp, " +
                            "end_timestamp," +
                            "mi, " +
                            "wall_time," +
                            "number_of_cores," +
                            "mips_per_machine " +
                            "FROM observed_jobs " +
                            "WHERE entry_timestamp >= ? AND entry_timestamp <= ?"
            );

            statement.setLong(1, startTime);
            statement.setLong(2, endTime);

            statement.execute();

            ResultSet resultSet = statement.getResultSet();

            logger.info("Reading cloudlets from the database");
            int id = 0;
            while (resultSet.next()) {
                long jobId = resultSet.getLong("job_id");
                long entryTimestamp = resultSet.getLong("entry_timestamp");
                long startTimestamp = resultSet.getLong("start_timestamp");
                long endTimestamp = resultSet.getLong("end_timestamp");
                long mi = resultSet.getLong("mi");
                long wallTime = resultSet.getLong("wall_time");
                int numberOfCores = resultSet.getInt("number_of_cores");
                double mipsPerMachine = resultSet.getDouble("mips_per_machine");

                logger.debug("Read: " + jobId + ": e: " + entryTimestamp + " mi: " + mi + " cores: " + numberOfCores);

                int startId = id;
                long submissionDelay = startTimestamp - entryTimestamp;
                while (numberOfCores > this.jobPeSplitThreshold) {
                    cloudlets.add(createCloudlet(id++, submissionDelay, mi, this.jobPeSplitThreshold));
                    numberOfCores -= this.jobPeSplitThreshold;
                }
                cloudlets.add(createCloudlet(id++, submissionDelay, mi, numberOfCores));

                logger.debug("Added: " + (id - startId) + " cloudlets");

            }
            logger.info("Read: " + cloudlets.size() + " cloudlets");

        } catch (SQLException e) {
            e.printStackTrace();
            caught = e;
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (caught != null) {
                throw new IOException(caught);
            }
        }

        return cloudlets;
    }

    private Cloudlet createCloudlet(int jobId, long submissionDelay, long mi, int numberOfCores) {
        Cloudlet cloudlet = new CloudletSimple(jobId, mi, numberOfCores)
                .setFileSize(DataCloudTags.DEFAULT_MTU)
                .setOutputSize(DataCloudTags.DEFAULT_MTU)
                .setUtilizationModel(new UtilizationModelFull());
        cloudlet.setSubmissionDelay(submissionDelay / speedUp);
        return cloudlet;
    }
//
//    @Override
//    public WorkloadReader setPredicate(Predicate<Cloudlet> predicate) {
//        throw new NotImplementedException("Not supported in DatabaseJobReader!");
//    }
}
