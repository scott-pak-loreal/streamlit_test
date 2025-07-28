import streamlit as st

# Title of the app
st.title("Basic Streamlit App")

# Display a simple text
st.write("Hello, Streamlit!")

# Add a slider widget
number = st.slider("Pick a number", 0, 100, 50)
st.write("You selected:", number)

# Add a button widget
if st.button("Greet"):
    st.write("👋 Hello there!")

# Add a text input
name = st.text_input("Enter your name")
if name:
    st.write(f"Welcome, {name}!")

# Display current status
st.write("App is running successfully.")