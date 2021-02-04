class SqlQualityTests:
    greater_than_0 = ("""
                        SELECT 
                            CASE WHEN total > 0 THEN 1 else 0 END AS NUM_ROWS
                            FROM
                            (
                            SELECT COUNT(*) AS total FROM {table_name}
                            ) num_rows""")
    
    count_nulls = ("""SELECT 
                      SUM( CASE WHEN {column_name} IS NULL THEN 1
                           ELSE 0 END ) AS NULL_COUNTS
                           
                       FROM {table_name}""")
    
                            