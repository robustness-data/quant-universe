import streamlit as st
import pandas as pd

def filter_dataframe(df):
    # Check if the session state for the filter list exists, if not initialize it
    if 'filter_list' not in st.session_state:
        st.session_state['filter_list'] = []

    # Function to add a new filter
    def add_filter():
        st.session_state['filter_list'].append({'column': None, 'values': []})

    # Display a button to add a new filter
    st.button("Add Filter", on_click=add_filter)

    # Create a container for each filter
    for i, filter_info in enumerate(st.session_state['filter_list']):
        with st.container():
            # Select column for the filter
            col = st.selectbox(f"Select column for filter {i+1}", df.columns.tolist(), key=f'column_{i}')
            st.session_state['filter_list'][i]['column'] = col

            # Multi-select for choosing filter values
            values = st.multiselect(f"Select values for {col}", df[col].unique(), key=f'values_{i}')
            st.session_state['filter_list'][i]['values'] = values

    # Select columns to display
    columns_to_show = st.multiselect("Select columns to display", df.columns.tolist(), default=df.columns.tolist())

    # Apply filters
    filtered_df = df.copy()
    for filter_info in st.session_state['filter_list']:
        if filter_info['column'] and filter_info['values']:
            filtered_df = filtered_df[filtered_df[filter_info['column']].isin(filter_info['values'])]

    # Show selected columns of the filtered dataframe
    return filtered_df[columns_to_show]

# Example usage in a Streamlit app
# df = pd.read_csv("your_data.csv")  # Replace with your DataFrame source
# filtered_df = filter_dataframe(df)
# st.write(filtered_df)