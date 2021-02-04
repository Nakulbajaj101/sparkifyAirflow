from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import DataQualityOperator
from helpers import SqlQualityTests




def quality_checks_subdag(parent_dag, child_dag, table_column_mapping, conn_id, args):
    
    with DAG(dag_id='%s.%s' % (parent_dag, child_dag), default_args=args) as dag:
        
        start_quality_tests=DummyOperator(task_id=f"{child_dag}_start")
        
        quality_tests={}
        for table_name, column_name in table_column_mapping.items():
            greater_than_0_sql=SqlQualityTests.greater_than_0.format(table_name=table_name)
            count_nulls_sql=SqlQualityTests.count_nulls.format(column_name=column_name,
                                                              table_name=table_name)
            quality_check_tasks=DataQualityOperator(
                                      task_id=f"{child_dag}_{table_name}",
                                      conn_id=conn_id,
                                      tests={"Greater Than 0 Rows":greater_than_0_sql,
                                             "Check No Null Rows":count_nulls_sql},
                                      test_results={"Greater Than 0 Rows":1,
                                                    "Check No Null Rows":0}
                                )
                                                    
            quality_tests[table_name]=quality_check_tasks
                                                    
        end_quality_tests=DummyOperator(task_id=f"{child_dag}_end")   
        
        for task_id in quality_tests.keys():
            quality_tests[task_id].set_upstream(start_quality_tests)
            quality_tests[task_id].set_downstream(end_quality_tests)
        
        return dag
            
         
             
                                      
                                      
                                      
                                                        
                                  
                                      
                                  
                                      
            
                

    
        
        