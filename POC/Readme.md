MS Fabric POC on Pharma:

I have used 4 files from patient perspective named as Patient, Encounters, Medications & Procedures.
The patient file has patient’s personal information as name, gender, DOB.
The encounter file has patient’s hospital visit details for each visit.
The medication file has patient’s medicine details as medicines prescribed during each visit.
The procedure file has medical procedure performed to a patient.
Medallion architecture:
1.	Bronze layer:
The raw data is ingested from amazon S3 to Fabric in the bronze_lakehouse. Data cleaning and transformations performed in the notebook and the cleaned data is saved to silver lakehouse.
2.	Silver layer:
The cleaned data is segregated to gold layer as:
I.	The patient details with his encounter details into the gold_encounter.
II.	Patient details with his medication details into gold_medication.
III.	Patient details with his procedure details into gold_procedure.  
3.	Gold layer:
Now the gold lakehouse has 3 files as gold_encounter, gold_medication, gold_procedure which are ready for the further analysis. 

 
This way, I implemented a Medallion architecture where raw data is ingested into Bronze, cleaned and standardized in Silver, and aggregated into Gold tables like Patient 360 for analytics. I ensured proper handling of nulls, avoided join explosion using aggregation, and stored results as Delta tables for efficient querying.


This is an auto-created file for POC
