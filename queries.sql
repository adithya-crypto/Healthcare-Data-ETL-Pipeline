-- 1. Verify tables exist
SHOW TABLES;

-- 2. Check table structures
DESCRIBE patients;
DESCRIBE doctors;
DESCRIBE appointments;

-- 3. Basic data counts
SELECT COUNT(*) as total_patients FROM patients;
SELECT COUNT(*) as total_doctors FROM doctors;
SELECT COUNT(*) as total_appointments FROM appointments;

-- 4. Doctor specialties distribution
SELECT 
    specialty,
    COUNT(*) as doctor_count
FROM doctors
GROUP BY specialty
ORDER BY doctor_count DESC;

-- 5. Appointment statistics by doctor
SELECT 
    d.doctor_name,
    d.specialty,
    COUNT(*) as appointment_count
FROM appointments a
JOIN doctors d ON a.doctor_id = d.doctor_id
GROUP BY d.doctor_name, d.specialty
ORDER BY appointment_count DESC;

-- 6. Follow-up analysis by specialty
SELECT 
    d.specialty,
    COUNT(*) as total_appointments,
    SUM(a.follow_up) as follow_ups,
    ROUND(AVG(a.follow_up) * 100, 2) as follow_up_percentage
FROM appointments a
JOIN doctors d ON a.doctor_id = d.doctor_id
GROUP BY d.specialty
ORDER BY follow_up_percentage DESC;

-- 7. Patient visit frequency
SELECT 
    p.patient_name,
    COUNT(*) as visit_count
FROM appointments a
JOIN patients p ON a.patient_id = p.patient_id
GROUP BY p.patient_name
ORDER BY visit_count DESC
LIMIT 10;

-- 8. Monthly appointment distribution
SELECT 
    DATE_FORMAT(appointment_datetime, '%Y-%m') as month,
    COUNT(*) as appointment_count
FROM appointments
GROUP BY month
ORDER BY month;