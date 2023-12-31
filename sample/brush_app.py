import pandas as pd
import plotly.express as px
import streamlit as st


# Function to wrap text
def wrap_text(text, char_limit, is_html=True):
    """Wrap text based on a character limit."""
    words = text.split()
    wrapped_text = ""
    line = ""
    for word in words:
        if len(line + ' ' + word) <= char_limit:
            line += ' ' + word if line else word
        else:
            if is_html:
                wrapped_text += line + '<br>'
            else:
                wrapped_text += line + '\n'
            line = word
    wrapped_text += line
    return wrapped_text


# Function to create the performance chart
def create_performance_chart(df, title, subtitle, source, note,
                             bg_color, bar_color, ylabel, xlabel,
                             char_limit, note_char_limit, x_ticks_rotation,
                             orientation='v', note_vertical_offset=0.2, source_vertical_offset=0.25,
                             **kwargs):
    # Apply text wrapping to x-axis labels and note
    df['wrapped_labels'] = df['id_col'].apply(lambda x: wrap_text(x, char_limit))
    wrapped_note = wrap_text(note, note_char_limit)

    if orientation == 'v':
        fig = px.bar(df, x='wrapped_labels', y='value_col', text='value_col',
                     title=f'{title}<br><sub>{subtitle}</sub>',
                     color_discrete_sequence=[bar_color])
    else:
        fig = px.bar(df, x='value_col', y='wrapped_labels', text='value_col',
                     title=f'{title}<br><sub>{subtitle}</sub>',
                     color_discrete_sequence=[bar_color])

    # Update the layout of the plot
    fig.update_layout(
        height=kwargs.get('height', 600),
        width=kwargs.get('width', 800),
        plot_bgcolor=bg_color,
        paper_bgcolor=bg_color,
        font_color='white',
        xaxis_title=xlabel,
        yaxis=dict(title=ylabel),
        annotations=[
            dict(xref='paper', yref='paper', x=0, y=-source_vertical_offset, showarrow=False, text=f"Source: {source}"),
            dict(xref='paper', yref='paper', x=0, y=-note_vertical_offset, showarrow=False, text=wrapped_note),
            
        ]
    )
    
    fig.update_traces(texttemplate='%{text:.2f}', textposition='outside')
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='grey')
    fig.update_xaxes(tickangle=x_ticks_rotation)
    fig.update_layout(showlegend=False)

    return fig


# Initialize session state variables if they don't exist
if 'data' not in st.session_state:
    st.session_state['data'] = None
if 'fig' not in st.session_state:
    st.session_state['fig'] = None

st.title('Interactive Bar Chart Generator with Plotly')

# File uploader
uploaded_file = st.file_uploader("Choose a CSV file", type="csv")
if uploaded_file is not None:
    st.session_state.data = pd.read_csv(uploaded_file)

# Input fields for customization
title = st.text_input('Enter the major title', 'Major Title Here')
subtitle = st.text_input('Enter the subtitle', 'Subtitle Here')
source = st.text_input('Enter the source', 'Source Here')
note = st.text_area('Enter the note', 'Note Here')
ylabel = st.text_input('Enter the y-axis label', 'Y-axis Label Here')
xlabel = st.text_input('Enter the x-axis label', 'X-axis Label Here')

# Color choice in the same row
col1, col2 = st.columns(2)
with col1:
    bg_color = st.color_picker('Choose background color', '#000000')
with col2:
    bar_color = st.color_picker('Choose bar color', '#FFA500')

# Advanced settings under an expandable banner
with st.expander("Advanced Settings"):
    col1_orientation, col2_width, col3_height = st.columns(3)
    orientation = col1_orientation.selectbox('Orientation', ['Vertical', 'Horizontal'])
    if orientation == 'Vertical':
        orientation = 'v'
    else:
        orientation = 'h'
    width = col2_width.number_input('Width', 100, 2000, 800)
    height = col3_height.number_input('Height', 100, 2000, 600)


    col1_xtick, col2_xtick = st.columns(2)
    char_limit = col1_xtick.slider('X-axis Label Width', 10, 50, 20)
    x_ticks_rotation = col2_xtick.slider('X-ticks Rotation Angle', 0, 90, 45)

    col1_notes, col2_notes, col3_notes = st.columns(3)
    note_char_limit = col1_notes.slider('Notes Pragraph Width', 30, 100, 50)
    note_vertical_offset = col2_notes.slider('Notes Vertical Offset', 0.1, 2.0, 0.2)
    source_vertical_offset = col3_notes.slider('Source Vertical Offset', 0.1, 2.0, 0.25)

# Plot button and figure regeneration
if st.button('Generate Chart') or st.session_state.data is not None:
    st.session_state.fig = create_performance_chart(df=st.session_state.data,
                                                    orientation=orientation,
                                                    height=height, width=width,
                                                    title=title,
                                                    subtitle=subtitle,
                                                    source=source,
                                                    note=note,
                                                    ylabel=ylabel,
                                                    xlabel=xlabel,
                                                    bg_color=bg_color,
                                                    bar_color=bar_color,
                                                    char_limit=char_limit,
                                                    note_char_limit=note_char_limit,
                                                    x_ticks_rotation=x_ticks_rotation,
                                                    note_vertical_offset=note_vertical_offset,
                                                    source_vertical_offset=source_vertical_offset)

# Display the figure
if st.session_state.fig:
    st.plotly_chart(st.session_state.fig, use_container_width=True)