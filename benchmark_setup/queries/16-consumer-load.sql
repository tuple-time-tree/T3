COPY consumer."EXPENDITURES" FROM '/data/consumer/EXPENDITURES.csv' DELIMITER '	' QUOTE '"' ESCAPE '\' NULL 'NULL' CSV HEADER;
COPY consumer."HOUSEHOLDS" FROM '/data/consumer/HOUSEHOLDS.csv' DELIMITER '	' QUOTE '"' ESCAPE '\' NULL 'NULL' CSV HEADER;
COPY consumer."HOUSEHOLD_MEMBERS" FROM '/data/consumer/HOUSEHOLD_MEMBERS.csv' DELIMITER '	' QUOTE '"' ESCAPE '\' NULL 'NULL' CSV HEADER;
