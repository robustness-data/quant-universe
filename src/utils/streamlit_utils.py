import streamlit as st
import pandas as pd

def filter_dataframe(df, default_cols=None, key=None):

    if key is None:
        import random
        key = random.randint(0, 2**32)

    # Check if the session state for the filter list exists, if not initialize it
    if f"filter_list_{key}" not in st.session_state:
        st.session_state[f"filter_list_{key}"] = []

    # Function to add a new filter
    def add_filter():
        st.session_state[f"filter_list_{key}"].append({'column': None, 'values': []})

    # Display a button to add a new filter
    st.button("Add Filter", on_click=add_filter, key=f"filter_button_{key}_{len(st.session_state[f'filter_list_{key}'])}")

    # Create a container for each filter
    for i, filter_info in enumerate(st.session_state[f"filter_list_{key}"]):
        with st.container():
            # Select column for the filter
            col = st.selectbox(f"Select column for filter {i+1}", df.columns.tolist(), key=f'column_{i}')
            st.session_state[f"filter_list_{key}"][i]['column'] = col

            # Multi-select for choosing filter values
            values = st.multiselect(f"Select values for {col}", df[col].unique(), key=f'values_{i}')
            st.session_state[f"filter_list_{key}"][i]['values'] = values

    # Select columns to display
    if default_cols is None:
        default_cols = df.columns.tolist()
    columns_to_show = st.multiselect("Select columns to display", df.columns.tolist(), default=default_cols)

    # Apply filters
    filtered_df = df.copy()
    for filter_info in st.session_state[f"filter_list_{key}"]:
        if filter_info['column'] and filter_info['values']:
            filtered_df = filtered_df[filtered_df[filter_info['column']].isin(filter_info['values'])]

    # Show selected columns of the filtered dataframe
    return filtered_df[columns_to_show]