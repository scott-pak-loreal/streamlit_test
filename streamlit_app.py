import streamlit as st
import pandas as pd

# Cache the data loading for performance
def load_data():
    return pd.read_excel('streamlit_testdata.xlsx', engine='openpyxl')

# Main application
def main():
    st.set_page_config(page_title='Revenue Calculator', layout='centered')
    st.title('Revenue Calculator MVP')

    # Load data
    df = load_data()

    # Brand selection dropdown
    brand = st.selectbox('Select Brand', options=df['Brand'].unique())

    # Filter data for the selected brand
    filtered = df[df['Brand'] == brand].reset_index(drop=True)

    # Budget input
    budget = st.number_input(
        'Budget', min_value=0.0, value=1000.0, step=100.0, format='%.2f'
    )

    # Format ROI for display (2 decimal places)
    filtered['Roi'] = filtered['Roi'].map(lambda x: f"{x:.2f}")

    # Display or calculate results
    if st.button('Calculate'):
        # Compute expected revenue
        filtered_numeric = df[df['Brand'] == brand].copy().reset_index(drop=True)
        filtered_numeric['Expected Revenue'] = filtered_numeric['Roi'] * budget
        # Format ROI and Expected Revenue
        display = filtered_numeric.copy()
        display['Roi'] = display['Roi'].map(lambda x: f"{x:.2f}")
        display['Expected Revenue'] = display['Expected Revenue'].map(lambda x: f"{x:,.0f}")

        st.subheader('Results')
        st.table(display[['Channel', 'Tactic', 'Roi', 'Expected Revenue']])
    else:
        # Show filtered data with formatted ROI
        st.subheader('Filtered Data')
        st.table(filtered[['Channel', 'Tactic', 'Roi']])

if __name__ == '__main__':
    main()