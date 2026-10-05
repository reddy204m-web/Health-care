# Run: streamlit run genai/app.py
import streamlit as st, pandas as pd
from text_to_sql import ask

st.set_page_config(page_title="Healthcare Usage Assistant", page_icon="🏥")
st.title("🏥 Healthcare Usage Assistant")
st.caption("Synthetic data only. Schema-only prompts, read-only SQL.")

examples = ["Which payer had the highest total claim cost?",
            "Top 10 providers by 30-day readmission rate with at least 20 discharges",
            "Average length of stay by age group for inpatient encounters",
            "How many ER encounters per year?"]
q = st.text_input("Ask about utilization, cost or readmissions", placeholder=examples[0])
st.write("Try:", " | ".join(f"`{e}`" for e in examples))

if q:
    with st.spinner("Thinking..."):
        try:
            out = ask(q)
            st.subheader("Answer"); st.write(out["answer"])
            st.subheader("Data"); st.dataframe(pd.DataFrame(out["rows"], columns=out["columns"]))
            with st.expander("Generated SQL"): st.code(out["sql"], language="sql")
        except Exception as ex:
            st.error(str(ex))
