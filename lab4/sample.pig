-- Load file: Make sure data is under same directory with .pig if using local mode or data is moved to HDFS if using mapreduce mode
inputLines = LOAD 'gaz_tracts_national.txt' AS (USPS:chararray, GEOID:chararray, POP10:int, "SOME_OTHER_COLUMNS");
inputLines = LOAD '/data/network_trace' USING PigStorage(' ') AS (time:chararray, "SOME_OTHER_COLUMNS")

-- Extract useful fields
extractLine = FOREACH inputLines GENERATE USPS, "DESIRED_COLUMNS";

-- Filter data
filteredLine = FILTER extractLine BY "SOME_CONDITION";

-- Order records
orderedLand = ORDER filteredLine BY "SOME_COLUMN" DESC;

-- Write to FS
STORE orderedLand INTO "SOMEWHERE";