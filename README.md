# bigquery-tablesummary
Returns summary-level statistics about a table

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
  

Please see complete documentation at: https://www.bigqueryblog.com/post/bigquery-table-summary
