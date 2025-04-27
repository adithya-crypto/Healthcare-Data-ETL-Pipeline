import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import datetime
import numpy as np

# --- Page Configuration ---
st.set_page_config(page_title="Healthcare Data Pipeline", layout="wide")

# --- Load Data ---
BASE_DIR = os.path.join(os.getcwd(), 'processed_data')

def load_data():
    patients = pd.read_csv(os.path.join(BASE_DIR, 'gold/patients.csv'))
    doctors = pd.read_csv(os.path.join(BASE_DIR, 'gold/doctors.csv'))
    appointments = pd.read_csv(os.path.join(BASE_DIR, 'gold/appointments.csv'))
    stats = pd.read_csv(os.path.join(BASE_DIR, 'gold/summary_stats.csv'))
    rejected = pd.read_csv(os.path.join(BASE_DIR, 'rejected/missing_patient_records.csv'))
    bronze = pd.read_csv(os.path.join(BASE_DIR, 'bronze/raw_health_data.csv'))
    silver = pd.read_csv(os.path.join(BASE_DIR, 'silver/cleaned_health_data.csv'))

    # Clean column names
    appointments.rename(columns=lambda x: x.strip().lower().replace(' ', '_'), inplace=True)
    doctors.rename(columns=lambda x: x.strip().lower().replace(' ', '_'), inplace=True)
    patients.rename(columns=lambda x: x.strip().lower().replace(' ', '_'), inplace=True)

    today = datetime.date.today()
    patients['patint_dob'] = pd.to_datetime(patients['patint_dob'], errors='coerce')
    patients['age'] = patients['patint_dob'].apply(lambda x: today.year - x.year if pd.notnull(x) else None)

    appointments['appointment_date_time'] = pd.to_datetime(appointments['appointment_date_time'], errors='coerce')
    appointments['appointment_date'] = appointments['appointment_date_time']

    np.random.seed(0)
    appointments['start_time'] = appointments['appointment_date_time']
    appointments['end_time'] = appointments['start_time'] + pd.to_timedelta(np.random.randint(30, 90, size=len(appointments)), unit='m')

    appointments = appointments.merge(doctors[['doctor_id', 'doctor_name', 'doctor_specialty']], on='doctor_id', how='left')

    return patients, doctors, appointments, stats, rejected, bronze, silver

patients, doctors, appointments, stats, rejected, bronze, silver = load_data()

# --- Sidebar Filters ---
st.sidebar.title('Filter Data')
doctor_filter = st.sidebar.selectbox('Select Doctor', options=['All'] + list(doctors['doctor_name'].unique()))

min_date = appointments['appointment_date'].min()
max_date = appointments['appointment_date'].max()
date_range = st.sidebar.date_input('Select Appointment Date Range', [min_date, max_date])

# Apply Filters
filtered_appointments = appointments.copy()
if doctor_filter != 'All':
    filtered_appointments = filtered_appointments[filtered_appointments['doctor_name'] == doctor_filter]
filtered_appointments = filtered_appointments[(filtered_appointments['appointment_date'] >= pd.to_datetime(date_range[0])) & (filtered_appointments['appointment_date'] <= pd.to_datetime(date_range[1]))]

# --- Title ---
st.title('Healthcare Data Pipeline Dashboard')
st.subheader('Comprehensive analysis of healthcare ETL outputs')

# --- Executive Summary ---
st.markdown("""
### Executive Summary
This dashboard provides an end-to-end visualization of a Healthcare Data Pipeline project. Raw patient appointment data is cleaned, validated, enriched with doctor information, and visualized to provide operational insights.
""")

# --- Top KPIs ---
st.markdown("### Key Metrics")
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Patients", f"{len(patients)}")
col2.metric("Total Doctors", f"{len(doctors)}")
col3.metric("Total Appointments", f"{len(appointments)}")
col4.metric("Rejected Records", f"{len(rejected)}")

# --- Patient Demographics ---
st.markdown("### Patient Demographics")
st.write("Analyzing the distribution of patient ages and overall demographics.")
col5, col6 = st.columns(2)
with col5:
    fig_age = px.histogram(patients, x='age', nbins=20, title="Patient Age Distribution")
    st.plotly_chart(fig_age, use_container_width=True)
with col6:
    if 'patient_gendr' in patients.columns:
        fig_gender = px.pie(patients, names='patient_gendr', title="Gender Distribution")
        st.plotly_chart(fig_gender, use_container_width=True)

# --- Appointment Timeline ---
st.markdown("### Appointment Timeline")
st.write("Visualizing scheduled appointments across doctors and time.")
fig_timeline = px.timeline(filtered_appointments, x_start='start_time', x_end='end_time', y='doctor_name', color='doctor_name', title='Appointments Scheduled by Doctor')
fig_timeline.update_yaxes(autorange="reversed")
st.plotly_chart(fig_timeline, use_container_width=True)

# --- Appointment Intensity Heatmap ---
st.markdown("### Appointment Intensity Heatmap")
st.write("Understanding peak periods for healthcare operations.")
filtered_appointments['day_of_week'] = filtered_appointments['appointment_date_time'].dt.day_name()
filtered_appointments['hour'] = filtered_appointments['appointment_date_time'].dt.hour
heatmap_data = filtered_appointments.groupby(['day_of_week', 'hour']).size().reset_index(name='count')
heatmap_pivot = heatmap_data.pivot(index='day_of_week', columns='hour', values='count').fillna(0)
fig_heatmap = px.imshow(heatmap_pivot, title='Appointment Load (Day vs Hour)')
st.plotly_chart(fig_heatmap, use_container_width=True)

# --- Doctor Specializations ---
st.markdown("### Doctor Specializations")
st.write("Distribution of doctor specialties across the healthcare network.")
fig_specialization = px.pie(doctors, names='doctor_specialty', title="Distribution of Doctor Specialties")
st.plotly_chart(fig_specialization, use_container_width=True)

# --- Doctor Performance Polar Chart ---
st.markdown("### Doctor Performance Overview")
st.write("Comparison of appointment handling across doctors.")
doctor_stats = filtered_appointments.groupby('doctor_name').agg({'appointment_date_time': 'count'}).reset_index()
doctor_stats.rename(columns={'appointment_date_time': 'Total Appointments'}, inplace=True)
fig_polar = go.Figure()
for idx, row in doctor_stats.iterrows():
    fig_polar.add_trace(go.Scatterpolar(r=[row['Total Appointments']], theta=['Total Appointments'], fill='toself', name=row['doctor_name']))
fig_polar.update_layout(polar=dict(radialaxis=dict(visible=True)), showlegend=True)
st.plotly_chart(fig_polar, use_container_width=True)

# --- Appointment Growth Over Time ---
st.markdown("### Appointment Growth Trend")
st.write("Tracking the number of appointments booked over weeks.")
filtered_appointments['week'] = filtered_appointments['appointment_date_time'].dt.isocalendar().week
weekly_counts = filtered_appointments.groupby('week').size().reset_index(name='appointments')
fig_growth = px.line(weekly_counts, x='week', y='appointments', title='Appointments Booked Per Week')
st.plotly_chart(fig_growth, use_container_width=True)

# --- Rejected Records Overview ---
st.markdown("### Rejected Records")
st.write("Reviewing records that failed the validation and cleaning stages.")
if not rejected.empty:
    st.dataframe(rejected)
else:
    st.success('No rejected records present.')

# --- Conclusion ---
st.markdown("""
### Conclusion
This Healthcare ETL Pipeline project demonstrates the transformation of raw operational data into actionable insights. From ingestion to visualization, the layered pipeline ensures data quality, governance, and business intelligence for improved healthcare operations.
""")

# --- Footer ---
st.caption('Healthcare Data Pipeline Dashboard | Developed with Streamlit and Plotly')
