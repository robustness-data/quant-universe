
import numpy as np
import pandas as pd
import statsmodels.api as sm
from sklearn.metrics import mean_absolute_error, mean_squared_error
from scipy.stats import jarque_bera

import matplotlib.pyplot as plt
import seaborn as sns


class DFM:
    def __init__(self, endog, k_factors=1, factor_order=1, error_order=1):
        """
        Initialize the DFM model.
        """
        self.endog = endog
        self.k_factors = k_factors
        self.factor_order = factor_order
        self.error_order = error_order
        self.model = None
        self.result = None
        self.train_data = None
        self.test_data = None

    # ------------------------------------------
    # In-Sample Fit
    # ------------------------------------------

    def fit(self):
        """
        Fit the DFM model.
        """
        self.model = sm.tsa.DynamicFactor(self.endog, k_factors=self.k_factors, factor_order=self.factor_order,
                                          error_order=self.error_order)
        self.result = self.model.fit()
        return self.result.summary()

    def evaluate_insample(self):
        """
        Evaluate in-sample fit using MAE, MSE, RMSE.
        """
        if self.result is None:
            raise ValueError("Model not fitted yet. Call the `fit` method first.")

        fitted_values = self.result.fittedvalues
        metrics = {}
        for col in self.endog.columns:
            true_values = self.endog[col]
            predicted_values = fitted_values[col]
            metrics[col] = {
                'MAE': mean_absolute_error(true_values, predicted_values),
                'MSE': mean_squared_error(true_values, predicted_values),
                'RMSE': np.sqrt(mean_squared_error(true_values, predicted_values))
            }
        self.in_sample_metrics = pd.DataFrame(metrics)

    # ------------------------------------------
    # Out-of-Sample Forecast
    # ------------------------------------------

    def split_data(self, test_size=0.2):
        """
        Split the data into training and test sets for out-of-sample forecast evaluation.
        """
        n = len(self.endog)
        cutoff = int(n * (1 - test_size))
        self.train_data = self.endog.iloc[:cutoff]
        self.test_data = self.endog.iloc[cutoff:]

    def forecast(self, steps=1):
        """
        Generate out-of-sample forecasts.
        """
        if self.result is None:
            raise ValueError("Model not fitted yet. Call the `fit` method first.")
        return self.result.forecast(steps=steps)

    def evaluate_outsample(self, test_size=0.2):
        """
        Evaluate out-of-sample forecasts using MAE, MSE, RMSE.
        """

        if self.result is None or self.test_data is None:
            raise ValueError("Model not fitted yet or data not split. Call the `fit` and `split_data` methods first.")

        self.split_data(test_size=test_size)

        forecast_values = self.result.forecast(steps=len(self.test_data))
        metrics = {}
        for col in self.endog.columns:
            true_values = self.test_data[col]
            predicted_values = forecast_values[col]
            metrics[col] = {
                'MAE': mean_absolute_error(true_values, predicted_values),
                'MSE': mean_squared_error(true_values, predicted_values),
                'RMSE': np.sqrt(mean_squared_error(true_values, predicted_values))
            }
        self.out_of_sample_metrics = pd.DataFrame(metrics)

    # ------------------------------------------
    # Diagnostics
    # ------------------------------------------

    def residual_plots(self):
        """
        Plot residuals for diagnostic purposes.
        """
        if self.result is None:
            raise ValueError("Model not fitted yet. Call the `fit` method first.")

        residuals = self.result.resid
        for col in residuals.columns:
            plt.figure(figsize=(12, 4))
            plt.title(f"Residuals for {col}")
            plt.plot(residuals[col])
            plt.show()

    def jarque_bera_test(self):
        """
        Perform Jarque-Bera test to check for normality of residuals.
        """
        if self.result is None:
            raise ValueError("Model not fitted yet. Call the `fit` method first.")

        jb_test = {}
        residuals = self.result.resid
        for col in residuals.columns:
            _, pvalue, _, _ = jarque_bera(residuals[col])
            jb_test[col] = pvalue
        return jb_test

    # ------------------------------------------
    # Comparisons
    # ------------------------------------------

    def compare_with_arima(self, arima_order=(1, 0, 1)):
        """
        Compare DFM model with a simpler ARIMA model using AIC and BIC.
        """
        from statsmodels.tsa.arima.model import ARIMA

        comparison_metrics = {}
        for col in self.endog.columns:
            # Fit ARIMA model
            arima_model = ARIMA(self.endog[col], order=arima_order)
            arima_result = arima_model.fit()

            # Get AIC and BIC for ARIMA
            aic_arima = arima_result.aic
            bic_arima = arima_result.bic

            # Get AIC and BIC for DFM (for the specific variable)
            aic_dfm = self.result.aic
            bic_dfm = self.result.bic

            comparison_metrics[col] = {
                'AIC_ARIMA': aic_arima,
                'BIC_ARIMA': bic_arima,
                'AIC_DFM': aic_dfm,
                'BIC_DFM': bic_dfm
            }
        return comparison_metrics

    # ------------------------------------------
    # Plotting Functionality
    # ------------------------------------------
    def plot_forecasts(self, steps=10):
        """
        Plot out-of-sample forecasts.
        """
        if self.result is None:
            raise ValueError("Model not fitted yet. Call the `fit` method first.")

        forecasts = self.result.forecast(steps=steps)
        for col in forecasts.columns:
            plt.figure(figsize=(12, 4))
            plt.title(f"Forecasts for {col}")
            plt.plot(self.endog.index, self.endog[col], label='True Values')
            plt.plot(self.endog.index[-steps:], forecasts[col], label='Forecasted Values', linestyle='--')
            plt.legend()
            plt.show()

    def plot_impulse_responses(self, steps=10):
        """
        Plot impulse response functions to show the effect of a one-unit shock to each factor.
        """
        if self.result is None:
            raise ValueError("Model not fitted yet. Call the `fit` method first.")

        irfs = self.result.impulse_responses(steps=steps)
        for col in irfs.columns:
            plt.figure(figsize=(12, 4))
            plt.title(f"Impulse Response Function for {col}")
            plt.plot(np.arange(steps), irfs[col])
            plt.xlabel('Steps')
            plt.ylabel('Response')
            plt.show()

    def plot_residual_density(self):
        """
        Plot the density of residuals for diagnostic purposes.
        """
        if self.result is None:
            raise ValueError("Model not fitted yet. Call the `fit` method first.")

        residuals = self.result.resid
        for col in residuals.columns:
            plt.figure(figsize=(12, 4))
            plt.title(f"Residual Density for {col}")
            sns.distplot(residuals[col], hist=False, kde=True, label='Density')
            plt.xlabel('Residual')
            plt.ylabel('Density')
            plt.legend()
            plt.show()

    def plot_acf_pacf(self, lags=40):
        """
        Plot the AutoCorrelation Function (ACF) and Partial AutoCorrelation Function (PACF) of residuals.
        """
        if self.result is None:
            raise ValueError("Model not fitted yet. Call the `fit` method first.")

        residuals = self.result.resid
        for col in residuals.columns:
            plt.figure(figsize=(12, 4))
            plt.title(f"ACF and PACF for {col}")

            plt.subplot(1, 2, 1)
            sm.graphics.tsa.plot_acf(residuals[col], lags=lags)
            plt.xlabel('Lag')
            plt.ylabel('ACF')

            plt.subplot(1, 2, 2)
            sm.graphics.tsa.plot_pacf(residuals[col], lags=lags)
            plt.xlabel('Lag')
            plt.ylabel('PACF')

            plt.tight_layout()
            plt.show()


class DFMConfig:
    def __init__(self, data: pd.DataFrame, k_factors=1, factor_order=1, error_order=1, variable_names=None):
        """
        Initialize the DFM configuration.

        Parameters:
        - data: DataFrame, the wide-format time series data
        - variable_names: list, names of variables to include in the DFM analysis
        - k_factors: int, the number of unobserved factors
        - factor_order: int, the order of the factor AR model
        - error_order: int, the order of the AR errors
        """
        if variable_names is None:
            variable_names = data.columns.tolist()
        self.data = data[variable_names]
        self.k_factors = k_factors
        self.factor_order = factor_order
        self.error_order = error_order

    def run_DFM(self):
        """
        Run the Dynamic Factor Model based on the given configuration.
        """
        # Initialize the DFM model
        dfm_model = DFM(
            endog=self.data,
            k_factors=self.k_factors,
            factor_order=self.factor_order,
            error_order=self.error_order
        )

        # Fit the model
        dfm_model.fit()

        # Return the fitted model for further analysis
        return dfm_model


def run_DFM():
    # Generate synthetic data for demonstration
    np.random.seed(123)
    T = 100  # Number of time periods
    data = pd.DataFrame({
        'Variable1': np.random.randn(T) + np.linspace(0, 10, T),
        'Variable2': np.random.randn(T) + np.linspace(5, 15, T),
        'Variable3': np.random.randn(T) + np.linspace(10, 20, T),
    })

    # Initialize and fit the DFM model
    dfm = DFM(endog=data, k_factors=1, factor_order=1, error_order=1)

    # Fit the model and print the summary
    print("Model Summary:")
    print(dfm.fit())

    # Split the data for out-of-sample evaluation
    dfm.split_data(test_size=0.2)

    # Generate and print 5-step ahead forecasts
    print("5-Step Ahead Forecasts:")
    print(dfm.forecast(steps=5))

    # Evaluate in-sample fit and print metrics
    print("In-sample Evaluation Metrics:")
    print(dfm.evaluate_insample())

    # Evaluate out-of-sample fit and print metrics
    print("Out-of-sample Evaluation Metrics:")
    print(dfm.evaluate_outsample())

    # Plot residual time series for diagnostics
    print("Plotting Residuals:")
    dfm.residual_plots()

    # Plot residual density for diagnostics
    print("Plotting Residual Density:")
    dfm.plot_residual_density()

    # Plot ACF and PACF for diagnostics
    print("Plotting ACF and PACF of Residuals:")
    dfm.plot_acf_pacf(lags=20)

    # Perform Jarque-Bera test for normality of residuals and print results
    print("Jarque-Bera Test for Normality of Residuals:")
    print(dfm.jarque_bera_test())

    # Compare with ARIMA model using AIC and BIC
    print("Comparison with ARIMA Model:")
    print(dfm.compare_with_arima(arima_order=(1, 0, 1)))

    # Plot 10-step ahead forecasts
    print("Plotting 10-Step Ahead Forecasts:")
    dfm.plot_forecasts(steps=10)

    # Plot Impulse Response Functions
    print("Plotting Impulse Response Functions:")
    dfm.plot_impulse_responses(steps=10)


def run_dfm_app():
    import streamlit as st
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
            dfm_config = DFMConfig(data=df.drop(columns=['date']), k_factors=k_factors, factor_order=factor_order,
                                   error_order=error_order, variable_names=available_vars)

            # Fit the model
            fitted_model = dfm_config.run_DFM()

            # Evaluation and Metrics
            st.subheader("DFM Evaluation Metrics")
            st.write("In-sample Evaluation Metrics:")
            st.write(fitted_model.evaluate_insample())
            st.write("Out-of-sample Evaluation Metrics:")
            st.write(fitted_model.evaluate_outsample())


if __name__ == '__main__':

    run_DFM()