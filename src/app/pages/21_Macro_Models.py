import os, sys, logging
from pathlib import Path
ROOT_DIR = Path(__file__).parent.parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))
    #sys.path.append(str(ROOT_DIR/'src'))

import src.config as cfg
import src.data.utils as hp
from src.model.macro.dfm import DFMConfig, run_dfm_app

import datetime
import itertools
from tqdm import tqdm
import pandas as pd
import streamlit as st
import plotly.express as px

# App title
st.title("Dynamic Factor Model (DFM) Analysis")

with st.expander("Background"):
    st.markdown(
        """
        # Dynamic Factor Models (DFM)

        ## Overview

        Dynamic Factor Models (DFM) are a subset of factor models specifically designed for analyzing multivariate time series data. These models are particularly useful when you have data sets with a large number of variables but a relatively limited number of observations. DFM aims to reduce dimensionality by representing the observed variables as linear combinations of a smaller number of unobserved factors and error terms.

        ## Mathematical Representation

        The standard DFM can be expressed as:

        $$
        X_t = \Lambda F_t + \epsilon_t
        $$

        Where:
        - $X_t$ is a $N \\times 1$ vector of observed variables at time $t$
        - $\Lambda$ is a $N \\times k$ matrix of factor loadings
        - $F_t$ is a $k \\times 1$ vector of unobserved common factors at time $t$
        - $\epsilon_t$ is a $N \\times 1$ vector of idiosyncratic errors at time $t$

        The dynamics in $F_t$ and $\epsilon_t$ are often modeled as autoregressive processes:

        $$
        F_t = A_1 F_{t-1} + \cdots + A_p F_{t-p} + u_t
        $$
        $$
        \epsilon_t = B_1 \epsilon_{t-1} + \cdots + B_q \epsilon_{t-q} + v_t
        $$

        ## Key Advantages

        - **Dimensionality Reduction**: DFM allows you to summarize the information in many variables by a fewer number of factors.
        - **Missing Data Handling**: DFM can cope with missing values in the observed variables.
        - **Forecasting**: DFM is well-suited for multi-step ahead forecasting, exploiting the dynamics among variables for better prediction accuracy.

        ## Applications

        DFMs are widely used in economics, finance, and other fields for tasks such as:
        - Economic indicator tracking
        - Asset pricing
        - Risk assessment
        - Anomaly detection in IoT sensors

        ## Limitations

        - **Interpretability**: The factors are unobserved and may not always have an intuitive interpretation.
        - **Computational Complexity**: Estimation can be computationally intensive for large datasets.

        """
    )

# Upload data
uploaded_file = st.file_uploader("Upload your wide-format DataFrame (CSV)", type=['csv'])
if uploaded_file is not None:
    df = pd.read_csv(uploaded_file)
    df['date'] = pd.to_datetime(df['date'])  # Convert 'date' column to datetime format
    df.set_index('date', inplace=True)  # Set 'date' column as index
    st.write("Data Preview:")
    st.write(df.head())

    # Validate columns
    non_float_cols = [col for col in df.columns if df[col].dtype != 'float64' and col != 'date']
    if non_float_cols:
        st.warning(f"The following columns are not of float type and will be dropped: {non_float_cols}")
        df = df.drop(columns=non_float_cols)

    # Drop null values and warn if any are found
    if df.isnull().values.any():
        st.warning("Null values found. They will be dropped.")
        df.dropna(inplace=True)

    # DFM Configuration
    st.subheader("DFM Configuration")

    df = df.diff().dropna()

    # Add multi-select for choosing variables
    available_vars = [col for col in df.columns if col != 'date']
    selected_vars = st.multiselect("Select Variables for DFM Analysis", available_vars, default=available_vars)

    col1, col2, col3 = st.columns(3)
    with col1:
        k_factors = st.number_input("Number of unobserved factors", 1, 10, 1)
    with col2:
        factor_order = st.number_input("Order of factor AR model", 1, 10, 1)
    with col3:
        error_order = st.number_input("Order of AR errors", 1, 10, 1)

    # Multi-select for metrics
    eval_metrics = st.multiselect("Evaluation Metrics",
                                  ['MAE', 'MSE', 'RMSE', 'ACF', 'PACF', 'Residual Plots', 'Forecast Plots'],
                                  ['MAE', 'MSE', 'RMSE'])

    if st.button("Run DFM"):
        # Initialize DFM Configuration
        dfm_config = DFMConfig(data=df, k_factors=k_factors, factor_order=factor_order,
                               error_order=error_order, variable_names=selected_vars)

        # Fit the model
        fitted_model = dfm_config.run_DFM()
        fitted_model.evaluate_insample()
        fitted_model.split_data(0.2)
        fitted_model.evaluate_outsample()
        st.write(fitted_model.in_sample_metrics)

        # Evaluation and Metrics
        st.subheader("DFM Evaluation Metrics")

        st.write(fitted_model.in_sample_metrics)
        st.write(fitted_model.out_of_sample_metrics)

        st.stop()
        # Simple metrics-value pair table
        simple_metrics = {}
        if 'MAE' in eval_metrics:
            simple_metrics['MAE'] = ['MAE']
        if 'MSE' in eval_metrics:
            simple_metrics['MSE'] = fitted_model.evaluate_insample()['MSE']
        if 'RMSE' in eval_metrics:
            simple_metrics['RMSE'] = fitted_model.evaluate_insample()['RMSE']

        st.table(pd.DataFrame(simple_metrics, index=['Value']))

        # Plotting metrics
        if 'ACF' in eval_metrics:
            st.write("ACF Plot")
            # Generate ACF plot using plotly
        if 'PACF' in eval_metrics:
            st.write("PACF Plot")
            # Generate PACF plot using plotly
        if 'Residual Plots' in eval_metrics:
            st.write("Residual Plots")
            # Generate Residual plots using plotly
        if 'Forecast Plots' in eval_metrics:
            st.write("Forecast Plots")
            # Generate Forecast plots using plotly
