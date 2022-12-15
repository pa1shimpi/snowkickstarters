import streamlit as st
import pandas as pd
import numpy as np
import snowflake.connector
from st_aggrid import AgGrid
import streamlit.components.v1 as com

#creating heading
com.html("""
<div>
<style>
h1.heading{
    color:SlateBlue;
    text-align:center;
    font-size:300%;
}
</style>
<h1 class="heading">
PIPELINE METADATA ANALYSIS
</h1>
""")
#st.title("PIPELINE METADATA ANALYSIS")

#connecting to snowflake
conn=snowflake.connector.connect(
    user='PAVANSHIMPI',
    account='szikhja-mr95455',
    password='No1@Snow',
    warehouse='SAMPLE_WH',
    database='TESTING',
    schema='TEST'
    )
com.html("""
<div>
<style>
h4.heading{
    color:Crimson;
    font-size:150%;
}
</style>
<h4 class="heading">
PIPELINE CORE STATISTICS
</h2>
""",height=70)
#st.subheader("PIPELINE CORE STATISTICS"
pipe_name = pd.read_sql("select distinct PIPE_NAME from TESTING.TEST.V_AGGREGATED_CREDITS_USAGE_PER_PIPE_1 ;",conn)
#st.subheader("PIPELINE CORE STATISTICS")
option = st.selectbox('PIPE_NAME:',pipe_name)
df=pd.read_sql("select * from TESTING.TEST.V_AGGREGATED_CREDITS_USAGE_PER_PIPE_1 A where PIPE_NAME=%(option)s ;",conn,params={"option":option})
hide_table_row_index = """
            <style>
            thead tr th:first-child {display:none}
            tbody th {display:none}
            </style>

            """
st.markdown(hide_table_row_index, unsafe_allow_html=True)
st.table(df)
#st.dataframe(df)
df2=pd.read_sql("select * from TESTING.TEST.V_AGGREGATED_CREDITS_USAGE_PER_PIPE_1;",conn)
st.bar_chart(data=df2,x='PIPE_NAME', y='AVG_CREDITS_USED', width=500, height=300, use_container_width=False)

#SECOND VIEW DASHBOARD
com.html("""
<div>
<style>
h4.heading{
    color:Crimson;
    font-size:150%;
}
</style>
<h4 class="heading">
PIPELINE EXECUTION STATUS
</h2>
""",height=70)
#st.subheader("PIPELINE EXECUTION STATUS")
df1 = pd.read_sql("select distinct PIPE_NAMES from TESTING.TEST.V_PIPE_STATUS_OF_LAST_10_EXECUTIONS ;",conn)
option = st.selectbox('PIPE_NAMES:',df1)
df2=pd.read_sql("select * from TESTING.TEST.V_PIPE_STATUS_OF_LAST_10_EXECUTIONS where PIPE_NAMES=%(option)s ;",conn,params={"option":option})
hide_table_row_index = """
            <style>
            thead tr th:first-child {display:none}
            tbody th {display:none}
            </style>

            """
st.markdown(hide_table_row_index, unsafe_allow_html=True)
st.table(df2)

#Third VIEW DASHBOARD
com.html("""
<div>
<style>
h4.heading{
    color:Crimson;
    font-size:150%;
}
</style>
<h4 class="heading">
QUERIES WITH HIGHEST RESOURCE CONSUMPTION
</h4>""",height=70)
#st.subheader('QUERIES WITH HIGHEST RESOURCE CONSUMPTION', anchor=None)
df1 = pd.read_sql("select distinct USER_NAME from TESTING.TEST.V_TOP_20_RESOURCE_UTILIZING_QUERIES_EXCL_PIPES ;",conn)
option = st.selectbox('USER NAME :',df1)
df2=pd.read_sql("select * from TESTING.TEST.V_TOP_20_RESOURCE_UTILIZING_QUERIES_EXCL_PIPES where USER_NAME=%(option)s ;",conn,params={"option":option})
st.table(df2)

#FORTH VIEW DASHBOARD
com.html("""
<div>
<style>
h4.heading{
    color:Crimson;
    font-size:150%;
}
</style>
<h4 class="heading">
RESOURCE USAGE
</h4>
""",height=70)
#st.subheader('RESOURCE USAGE', anchor=None)
df1 = pd.read_sql("select *  from TESTING.TEST.V_TOTAL_RESOURCE_USAGE_PER_USER_EXCL_PIPES ;",conn)
hide_table_row_index = """
            <style>
            thead tr th:first-child {display:none}
            tbody th {display:none}
            </style>

            """
st.markdown(hide_table_row_index, unsafe_allow_html=True)
st.table(df1)

#FORTH VIEW DASHBOARD
com.html("""
<div>
<style>
h4.heading{
    color:Crimson;
    font-size:150%;
}
</style>
<h4 class="heading">
INGESTION STATS
</h4>
""",height=70)
#st.subheader(' INGESTION STATS', anchor=None)
df1 = pd.read_sql("select *  from TESTING.TEST.V_BULKCOPY_VS_PIPES_INSTANCES ;",conn)
hide_table_row_index = """
            <style>
            thead tr th:first-child {display:none}
            tbody th {display:none}
            </style>

            """
st.markdown(hide_table_row_index, unsafe_allow_html=True)
st.table(df1)
st.snow()
