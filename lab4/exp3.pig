--load files
ip_trace = LOAD '/lab4/datasets/ip_trace' USING PigStorage(' ') AS (time:chararray, id:long, srcip:chararray, bracket:chararray, destip:chararray, protocol:chararray, depdata:chararray);
raw_block = LOAD '/lab4/datasets/raw_block' USING PigStorage(' ') AS (id:long, status:chararray);

--find blocked ones 
blocked = FILTER raw_block BY status == 'Blocked';

--intersect the two
joined = JOIN ip_trace BY id, blocked BY id;

--keep atributes we need
firewall = FOREACH joined GENERATE time, ip_trace::id AS id, srcip, destip, status;

--output
STORE firewall INTO '/lab6/exp3/firewall' USING PigStorage(' ');

--group and count
groups = GROUP firewall BY srcip;
totals = FOREACH groups GENERATE group, COUNT(firewall) AS count;

temp = FILTER totals BY (count <= 6000);

--sort and store
ordered = ORDER totals BY count DESC;
STORE ordered INTO '/lab6/exp3/output' USING PigStorage('\t');