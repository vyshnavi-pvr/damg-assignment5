import json
import altair as alt
import pandas as pd
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import col
import streamlit as st


import sys
sys.path.append('..')

def create_session():
    if "snowpark_session" not in st.session_state:
        session = Session.builder.configs(json.load(open("connection.json"))).create()
        st.session_state['snowpark_session'] = session
    else:
        session = st.session_state['snowpark_session']
    return session

@st.experimental_memo(show_spinner=False)
def load_data():
    historical_data = session.table("BUDGET_ALLOCATIONS_AND_ROI").unpivot("Budget", "Channel", ["SearchEngine", "SocialMedia", "Video", "Email"]).filter(col("MONTH") != "July")
    df_last_six_months_allocations = historical_data.drop("ROI").to_pandas()
    df_last_six_months_roi = historical_data.drop(["CHANNEL", "BUDGET"]).distinct().to_pandas()
    df_last_months_allocations = historical_data.filter(col("MONTH") == "June").to_pandas()
    return historical_data.to_pandas(), df_last_six_months_allocations, df_last_six_months_roi, df_last_months_allocations


st.write("<style>[data-testid='stMetricLabel'] {min-height: 0.5rem !important}</style>", unsafe_allow_html=True)
st.title("Return Over Investment for the budget spent on Adverstisement")
session = create_session()
historical_data, df_last_six_months_allocations, df_last_six_months_roi, df_last_months_allocations = load_data()

st.header("Advertising budgets")
col1, _, col2 = st.columns([4, 1, 4])
channels = ["Search engine", "Social media", "Email", "Video"]
budgets = []
for channel, default, col in zip(channels, df_last_months_allocations["BUDGET"].values, [col1, col1, col2, col2]):
    with col:
        budget = st.slider(channel, 0, 100, int(default), 5)
        budgets.append(budget)

st.header("Predicted revenue")

@st.experimental_memo(show_spinner=False)
def predict(budgets):
    df_predicted_roi = session.sql(f"SELECT predict_roi(array_construct({budgets[0]*1000},{budgets[1]*1000},{budgets[2]*1000},{budgets[3]*1000})) as PREDICTED_ROI").to_pandas()
    predicted_roi, last_month_roi = df_predicted_roi["PREDICTED_ROI"].values[0] / 100000, df_last_six_months_roi["ROI"].iloc[-1]
    change = round((predicted_roi - last_month_roi) / last_month_roi * 100, 1)
    return predicted_roi, change

predicted_roi, change = predict(budgets)
st.metric("", f"$ {predicted_roi:.2f} million", f"{change:.1f} % vs last month")


        
# running streamlit

# studiolab_domain = 'qrosew9ktffgtnu'

# launch
# if studiolab_domain:
#     studiolab_region = 'us-east-2'
#     url = f'https://{studiolab_domain}.studio.{studiolab_region}.sagemaker.aws/studiolab/default/jupyter/proxy/6006/'
    
# else: 
    
#     url = f'http://127.0.0.1:6006'

# print(f'Wait a few seconds and then click the link below to open your Streamlit application \n{url}\n')

# streamlit run --theme.base dark Snowpark_For_Python.py --server.port 6006 \
#                                                           --server.address 127.0.0.1 \
#                                                           --server.headless true


# https://qrosew9ktffgtnu.studio.us-east-2.sagemaker.aws/studiolab/default/jupyter/proxy/6006/