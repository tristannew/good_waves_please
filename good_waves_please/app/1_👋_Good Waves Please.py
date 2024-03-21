import streamlit as st

st.title("🏄 :rainbow[Good Waves Please] 🏄")

st.markdown(
    """
            Welcome to Good Waves Please!\n
            This is a little applet that I have made which collects information
            on your surfs. It collects information on how much you enjoyed the surf, and what is was like
            from your perspective, as well as what the Surfline forecast said for that day.\n
            My hope is that eventually there will be a database of surf sessions that we have all had
            and we would be able to see what conditions we most enjoyed at which spots.
            """
)

data_input = st.button("Go to rate a surf.")
if data_input:
    st.switch_page("pages/2_🌊_How was your surf?.py")

# wave_pred = st.button("Go to find out where to surf.")
# if wave_pred:
#     st.switch_page("pages/wave_prediction.py")
