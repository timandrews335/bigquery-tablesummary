CREATE OR REPLACE PROCEDURE misc.table_summary(tbl_name STRING, display_bools_as_bools BOOL)
BEGIN

/*
Tim Andrews 20922-07-16  https://bigqueryblog.com

Takes in a table in dataset.tablename format, plus an argument whether to display statustics about bools as bools, or keep them numeric to provide insights into the distribution of bools.
For each column in a table, the following information is returns:
  1.  Ordinal position
  2.  Data type
  3.  Mininum value in the table
  4.  First quartile in the table (excluding NULLs)
  5.  Median in the table (excluding NULLs)
  6.  Mean in the table (excluding NULLs, obviously)
  7.  Third quartile in the table (excluding NULLs)
  8.  Max value in the table
  9.  Approximate distinct values for the column in the table
  10. The count of NULL values for the column in the table
  11. The total count of values for the column in the table (useful for calculating NULL pct.)

  For STRINGS,BYTES and ARRAYS, the LENGTH is calculated for the numeric distribution-type metrics (MIN, MAX, MEDIAN, etc.)
  


*/

DECLARE sql_select STRING;
DECLARE sql_insert STRING;
DECLARE base_date STRING;


SET base_date = '1900-01-01';

/* Create a SQL string which will interrogate the rows for each column, including placeholders to swap in column names and table names */
SET sql_select = '''
INSERT INTO temp_processed_rows
SELECT
"<<alwayscolumn>>" AS col_name

, CAST(<<ordinalposition>> AS INT64) AS ordinal_position

,"<<datatype>>" AS data_type

,CAST(MIN(<<column>>) AS STRING) AS min_val

,(SELECT CAST(<<alwayscolumn>> AS STRING) FROM (
  SELECT  NTILE(4) OVER(ORDER BY <<column>>) AS nt, <<column>> AS <<alwayscolumn>>
  FROM <<table>> WHERE <<column>> IS NOT NULL
) x
WHERE nt = 1
QUALIFY ROW_NUMBER() OVER (ORDER BY <<alwayscolumn>> DESC) = 1
) AS first_quartile

,(SELECT CAST(<<alwayscolumn>> AS STRING) FROM (
  SELECT  NTILE(4) OVER(ORDER BY <<column>>) AS nt, <<column>> AS <<alwayscolumn>>
  FROM <<table>>  WHERE <<column>> IS NOT NULL
) x
WHERE nt = 2
QUALIFY ROW_NUMBER() OVER (ORDER BY <<alwayscolumn>> DESC) = 1
) AS median

,CAST(AVG((<<column>>)) AS STRING) AS mean_val

,(SELECT CAST(<<alwayscolumn>> AS STRING) FROM (
  SELECT  NTILE(4) OVER(ORDER BY <<column>>) AS nt, <<column>> AS <<alwayscolumn>>
  FROM <<table>>  WHERE <<column>> IS NOT NULL
) x
WHERE nt = 3
QUALIFY ROW_NUMBER() OVER (ORDER BY <<alwayscolumn>> DESC) = 1
) AS third_quartile

,CAST(MAX(<<column>>) AS STRING) AS max_val

,APPROX_COUNT_DISTINCT(<<alwayscolumn>>) AS distinct_vals

,COUNT(CASE WHEN <<column>> IS NULL THEN 1 ELSE NULL END) AS null_vales

,COUNT(1) AS total_count

FROM <<table>>;
''';

/* Use the SQL string to load a version of the string into a temp table, swapping out for row and column names.
This temp table will be looped over to process each column
*/
CREATE OR REPLACE TEMPORARY TABLE temp_data_to_process
AS
SELECT REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(sql_select, '<<column>>'
  , 
  CASE
    WHEN data_type = 'INT64' THEN column_name
    WHEN data_type = 'FLOAT64' THEN column_name
    WHEN data_type LIKE '%NUMERIC%' THEN column_name
    WHEN data_type = 'STRING' THEN CONCAT('LENGTH(', column_name, ')')
    WHEN data_type LIKE '%BYTE%' THEN CONCAT('LENGTH(', column_name, ')')
    WHEN data_type LIKE '%ARRAY%' THEN CONCAT('ARRAY_LENGTH(', column_name, ')')
    WHEN data_type LIKE '%TIMESTAMP%' THEN CONCAT('DATETIME_DIFF(', column_name, ", TIMESTAMP '1900-01-01', MICROSECOND)")
    WHEN data_type LIKE '%DATETIME%' THEN CONCAT('DATETIME_DIFF(', column_name, ", DATETIME '1900-01-01', MICROSECOND)")
    WHEN data_type LIKE '%DATE%' THEN CONCAT('DATE_DIFF(', column_name, ", DATE '1900-01-01', DAY)")
    WHEN data_type LIKE '%TIME%' THEN CONCAT('TIME_DIFF(', column_name, ", TIME '00:00:00', MICROSECOND)")
    WHEN data_type LIKE '%BOOL%' THEN CONCAT('CAST(', column_name, ' AS INT64)')
  END 
  ), '<<table>>', tbl_name), '<<alwayscolumn>>', column_name), "<<datatype>>", data_type), "<<ordinalposition>>", CAST(ordinal_position AS STRING)) AS sql_to_run
  ,data_type
  ,0 AS processed
FROM misc.INFORMATION_SCHEMA.COLUMNS
WHERE CONCAT(table_schema, '.', table_name) = tbl_name
AND 
(
  data_type LIKE '%INT%'
  OR data_type LIKE '%FLOAT%'
  OR data_type LIKE '%NUMERIC%'
  OR data_type LIKE '%STRING%'
  OR data_type LIKE '%BYTE%'
  OR data_type LIKE '%%ARRAY'
  OR data_type LIKE '%TIME%'
  OR data_type LIKE '%DATE%'
  OR data_type LIKE '%BOOL%'
);


/* Loop over the table of prepared SQL strings, executing them, and storing the statistical results in a temp table */
CREATE OR REPLACE TEMPORARY TABLE temp_processed_rows
(
   col_name STRING
  ,ordinal_position INT64
  ,data_type STRING
  ,min_val STRING
  ,first_quartile STRING
  ,median STRING
  ,mean_val STRING
  ,third_quartile STRING
  ,max_val STRING
  ,distinct_vals INT64
  ,null_vals INT64
  ,total_count INT64
);

WHILE (SELECT COUNT(1) FROM temp_data_to_process WHERE processed = 0) > 0 DO
  SET sql_insert = (SELECT MIN(sql_to_run) FROM temp_data_to_process WHERE processed = 0);
  EXECUTE IMMEDIATE sql_insert;
  UPDATE temp_data_to_process SET processed = 1 WHERE sql_to_run = sql_insert;
END WHILE;

SELECT * FROM temp_processed_rows;

/* Update the temp table to turn things like dates and bools back into their native formats, so they don't look like ints */

UPDATE temp_processed_rows
SET min_val = 
CASE 
  WHEN data_type = 'TIMESTAMP' THEN CAST(TIMESTAMP_ADD(CAST(base_date AS TIMESTAMP), INTERVAL CAST(min_val AS INT64) MICROSECOND ) AS STRING)
  WHEN data_type = 'DATETIME' THEN CAST(DATETIME_ADD(CAST(base_date AS DATETIME), INTERVAL CAST(min_val AS INT64) MICROSECOND ) AS STRING)
  WHEN data_type = 'DATE' THEN CAST(DATE_ADD(CAST(base_date AS DATE), INTERVAL CAST(ROUND(CAST(min_val AS FLOAT64), 0) AS INT64) DAY ) AS STRING)
  WHEN data_type = 'TIME' THEN CAST(TIME_ADD(CAST('00:00:00' AS TIME), INTERVAL CAST(min_val AS INT64) MICROSECOND ) AS STRING)
  WHEN data_type = 'BOOL' AND  display_bools_as_bools THEN CASE WHEN min_val IS NULL THEN NULL WHEN CAST(min_val AS FLOAT64) < .5 THEN 'FALSE' ELSE 'TRUE' END
  ELSE min_val
END 
,first_quartile =
CASE
  WHEN data_type = 'TIMESTAMP' THEN CAST(TIMESTAMP_ADD(CAST(base_date AS TIMESTAMP), INTERVAL CAST(first_quartile AS INT64) MICROSECOND )  AS STRING)
  WHEN data_type = 'DATETIME' THEN CAST(DATETIME_ADD(CAST(base_date AS DATETIME), INTERVAL CAST(first_quartile AS INT64) MICROSECOND )  AS STRING)
  WHEN data_type = 'DATE' THEN CAST(DATE_ADD(CAST(base_date AS DATE), INTERVAL CAST(ROUND(CAST(first_quartile AS FLOAT64), 0) AS INT64) DAY )  AS STRING)
  WHEN data_type = 'TIME' THEN CAST(TIME_ADD(CAST('00:00:00' AS TIME), INTERVAL CAST(first_quartile AS INT64) MICROSECOND )  AS STRING)
  WHEN data_type = 'BOOL' AND  display_bools_as_bools THEN CASE WHEN first_quartile IS NULL THEN NULL  WHEN CAST(first_quartile AS FLOAT64) < .5 THEN 'FALSE' ELSE 'TRUE' END
  ELSE first_quartile
END
,median = 
CASE 
  WHEN data_type = 'TIMESTAMP' THEN CAST(TIMESTAMP_ADD(CAST(base_date AS TIMESTAMP), INTERVAL CAST(median AS INT64) MICROSECOND )  AS STRING)
  WHEN data_type = 'DATETIME' THEN CAST(DATETIME_ADD(CAST(base_date AS DATETIME), INTERVAL CAST(median AS INT64) MICROSECOND )  AS STRING)
  WHEN data_type = 'DATE' THEN CAST(DATE_ADD(CAST(base_date AS DATE), INTERVAL CAST(ROUND(CAST(median AS FLOAT64), 0) AS INT64) DAY )  AS STRING)
  WHEN data_type = 'TIME' THEN CAST(TIME_ADD(CAST('00:00:00' AS TIME), INTERVAL CAST(median AS INT64) MICROSECOND )  AS STRING)
  WHEN data_type = 'BOOL' AND  display_bools_as_bools THEN CASE WHEN median IS NULL THEN NULL WHEN CAST(median AS FLOAT64) < .5 THEN 'FALSE' ELSE 'TRUE' END
  ELSE median
END
,mean_val =
CASE
  WHEN data_type = 'TIMESTAMP' THEN CAST(TIMESTAMP_ADD(CAST(base_date AS TIMESTAMP), INTERVAL CAST(mean_val AS INT64) MICROSECOND )  AS STRING)
  WHEN data_type = 'DATETIME' THEN CAST(DATETIME_ADD(CAST(base_date AS DATETIME), INTERVAL CAST(mean_val AS INT64) MICROSECOND )  AS STRING)
  WHEN data_type = 'DATE' THEN CAST(DATE_ADD(CAST(base_date AS DATE), INTERVAL CAST(ROUND(CAST(mean_val AS FLOAT64), 0) AS INT64) DAY )  AS STRING)
  WHEN data_type = 'TIME' THEN CAST(TIME_ADD(CAST('00:00:00' AS TIME), INTERVAL CAST(mean_val AS INT64) MICROSECOND )  AS STRING)
  WHEN data_type = 'BOOL' AND  display_bools_as_bools THEN CASE WHEN mean_val IS NULL THEN NULL WHEN CAST(mean_val AS FLOAT64) < .5 THEN 'FALSE' ELSE 'TRUE' END
  ELSE mean_val
END
,third_quartile =
CASE 
  WHEN data_type = 'TIMESTAMP' THEN CAST(TIMESTAMP_ADD(CAST(base_date AS TIMESTAMP), INTERVAL CAST(third_quartile AS INT64) MICROSECOND )  AS STRING)
  WHEN data_type = 'DATETIME' THEN CAST(DATETIME_ADD(CAST(base_date AS DATETIME), INTERVAL CAST(third_quartile AS INT64) MICROSECOND )  AS STRING)
  WHEN data_type = 'DATE' THEN CAST(DATE_ADD(CAST(base_date AS DATE), INTERVAL CAST(ROUND(CAST(third_quartile AS FLOAT64), 0) AS INT64) DAY )  AS STRING)
  WHEN data_type = 'TIME' THEN CAST(TIME_ADD(CAST('00:00:00' AS TIME), INTERVAL CAST(third_quartile AS INT64) MICROSECOND )  AS STRING)
  WHEN data_type = 'BOOL' AND  display_bools_as_bools THEN CASE WHEN third_quartile IS NULL THEN NULL WHEN CAST(third_quartile AS FLOAT64) < .5 THEN 'FALSE' ELSE 'TRUE' END
  ELSE third_quartile
END
,max_val =
CASE 
  WHEN data_type = 'TIMESTAMP' THEN CAST(TIMESTAMP_ADD(CAST(base_date AS TIMESTAMP), INTERVAL CAST(max_val AS INT64) MICROSECOND )  AS STRING)
  WHEN data_type = 'DATETIME' THEN CAST(DATETIME_ADD(CAST(base_date AS DATETIME), INTERVAL CAST(max_val AS INT64) MICROSECOND )  AS STRING)
  WHEN data_type = 'DATE' THEN CAST(DATE_ADD(CAST(base_date AS DATE), INTERVAL CAST(ROUND(CAST(max_val AS FLOAT64), 0) AS INT64) DAY )  AS STRING)
  WHEN data_type = 'TIME' THEN CAST(TIME_ADD(CAST('00:00:00' AS TIME), INTERVAL CAST(max_val AS INT64) MICROSECOND )  AS STRING)
  WHEN data_type = 'BOOL' AND  display_bools_as_bools THEN CASE WHEN max_val IS NULL THEN NULL WHEN CAST(max_val AS FLOAT64) < .5 THEN 'FALSE' ELSE 'TRUE' END
  ELSE max_val
END
WHERE 1 = 1;

SELECT * FROM temp_processed_rows ORDER BY ordinal_position;
END
