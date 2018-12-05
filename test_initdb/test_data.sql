CREATE TABLE IF NOT EXISTS observed_jobs (
  job_id SERIAL PRIMARY KEY,
  entry_timestamp BIGINT,
  start_timestamp BIGINT,
  end_timestamp BIGINT,
  mi BIGINT, -- MILLION INSTRUCTIONS AS TIME MEASUREMENT
  wall_time BIGINT,
  number_of_cores INTEGER,
  mips_per_machine NUMERIC 
);

INSERT INTO observed_jobs (
  entry_timestamp,
  start_timestamp,
  end_timestamp,
  mi,
  wall_time,
  number_of_cores,
  mips_per_machine
)
VALUES
(1544026148012, 1544026148013, -1, 100, 1, 1, 100), -- job which was executed only for a second
(1544026148012, 1544026148015, -1, 120000, 10, 10, 1200) -- job which was executed only for a 10 sec over 10 cores
;
