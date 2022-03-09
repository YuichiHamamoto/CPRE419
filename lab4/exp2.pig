--load the file
input_lines = LOAD '/lab4/datasets/network_trace' USING PigStorage(' ') AS (time:chararray, IP:chararray, srcip:chararray, arrow:chararray, destip:chararray, protocol:chararray, depdata:chararray);

--filter by tcp
tcps = FILTER input_lines BY protocol == 'tcp';

--remove the last part of the IPs
pairs = FOREACH tcps GENERATE SUBSTRING(srcip, 0, LAST_INDEX_OF(srcip, '.')) AS srcip, SUBSTRING(destip, 0, LAST_INDEX_OF(destip, '.')) AS destip;

--remove duplicates
sources = DISTINCT pairs;

--group by src ip and count dest ips
temp = GROUP sources BY srcip;
totals = FOREACH temp GENERATE group, COUNT(sources) AS count;

--order, limit, store
ordered = ORDER totals BY count DESC;
top = LIMIT ordered 10;
STORE top INTO '/lab6/exp2/output/';