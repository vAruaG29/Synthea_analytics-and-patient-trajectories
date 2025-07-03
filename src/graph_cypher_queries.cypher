// This script creates nodes and relationships in a Neo4j graph database from CSV files containing patient, procedure, condition, and encounter data.
// It uses the LOAD CSV command to read the data from the CSV files and the MERGE command to create nodes and relationships in the graph.
// The script assumes that the Neo4j database is already set up and running, and that the CSV files are accessible via the provided URLs.

// dummy data to test the script and to get the structure right
// patients_csv = 'https://raw.githubusercontent.com/vAruaG29/check/refs/heads/main/patients.csv';
// procedures_csv = 'https://raw.githubusercontent.com/vAruaG29/check/refs/heads/main/unique_procedure_nodes.csv';
// conditions_csv = 'https://raw.githubusercontent.com/vAruaG29/check/refs/heads/main/unique_condition_nodes.csv';
// encounters_csv = 'https://raw.githubusercontent.com/vAruaG29/check/refs/heads/main/filtered_encounters.csv';
// first_encounters_csv = 'https://raw.githubusercontent.com/vAruaG29/check/refs/heads/main/first_encounters.csv';
// chained_encounters_csv = 'https://raw.githubusercontent.com/vAruaG29/check/refs/heads/main/chained_encounters_patient_specific.csv';
// selected_patients_conditions_csv = 'https://raw.githubusercontent.com/vAruaG29/check/refs/heads/main/selected_patients_conditions.csv';
// selected_patients_procedures_csv = 'https://raw.githubusercontent.com/vAruaG29/check/refs/heads/main/selected_patients_procedures.csv';

// Create patient nodes
// This will create nodes for each patient with their details
// The CSV file should contain patient details with columns: Id, FIRST, LAST, GENDER, RACE, BIRTHDATE

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/vAruaG29/check/refs/heads/main/patients.csv' AS row
MERGE (p:Patient {id: row.Id})
SET p.firstName = row.FIRST,
    p.lastName = row.LAST,
    p.gender = row.GENDER,
    p.race = row.RACE,
    p.birthDate = row.BIRTHDATE;


// Create procedure nodes
// This will create nodes for each unique procedure code and its description
// The CSV file should contain unique procedure codes and their descriptions
// The file should have two columns: CODE and DESCRIPTION

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/vAruaG29/check/refs/heads/main/unique_procedure_nodes.csv' AS row
MERGE (proc:Procedure {id: row.CODE})
SET proc.description = row.DESCRIPTION;


// Create condition nodes
// This will create nodes for each unique condition code and its description
// The CSV file should contain unique condition codes and their descriptions
// The file should have two columns: CODE and DESCRIPTION

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/vAruaG29/check/refs/heads/main/unique_condition_nodes.csv' AS row
MERGE (cond:Condition {id: row.CODE})
SET cond.description = row.DESCRIPTION;



// Create encounter nodes
// This will create nodes for each encounter with its details
// The CSV file should contain encounter details with columns: Id, DESCRIPTION, START, PATIENT
// ENCOUNTERCLASS, CODE, REASONCODE, REASONDESCRIPTION, procedure_set

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/vAruaG29/check/refs/heads/main/filtered_encounters.csv' AS row
MERGE (e:Encounter {id: row.Id})
SET e.name = row.DESCRIPTION,
    e.start = row.START,
    e.patient = row.PATIENT,
    e.encounterClass = row.ENCOUNTERCLASS,
    e.code = row.CODE,
    e.description = row.DESCRIPTION,
    e.reasonCode = row.REASONCODE,
    e.reasonDescription = row.REASONDESCRIPTION,
    e.procedureSet = row.procedure_set;



// Create relationships

// First encounter node
// This will create a relationship between the patient and their first encounter
// The CSV file should contain the first encounter details with columns: PATIENT, ENCOUNTER

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/vAruaG29/check/refs/heads/main/first_encounters.csv' AS row
MATCH (p:Patient {id: row.PATIENT})
MATCH (e:Encounter {id: row.ENCOUNTER_ID})
MERGE (p)-[:FIRST_ENCOUNTER]->(e);


// Create chained encounter but after the first encounter and next encounter
// This will create relationships between encounters based on the chained encounters data
// The CSV file should contain chained encounter details with columns: ENCOUNTER_ID_1, ENCOUNTER_ID_2
// It assumes that the encounters are already created and linked to patients

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/vAruaG29/check/refs/heads/main/chained_encounters_patient_specific.csv' AS row
MATCH (e1:Encounter {id: row.ENCOUNTER_ID_1})
MATCH (e2:Encounter {id: row.ENCOUNTER_ID_2})
MERGE (e1)-[:NEXT_ENCOUNTER]->(e2);


// Make relation between encounter and conditions names as documented conditions
// This will create relationships between encounters and conditions based on the selected patients' conditions
// The CSV file should contain selected patients' conditions with columns: ENCOUNTER, CODE

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/vAruaG29/check/refs/heads/main/selected_patients_conditions.csv' AS row
MATCH (e:Encounter {id: row.ENCOUNTER})
MATCH (c:Condition {id: row.CODE})
MERGE (e)-[:HAS_CONDITION]->(c);

// Make relation between encounter and procedures names as documented procedures
// This will create relationships between encounters and procedures based on the selected patients' procedures
// The CSV file should contain selected patients' procedures with columns: ENCOUNTER, CODE

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/vAruaG29/check/refs/heads/main/selected_patients_procedures.csv' AS row
MATCH (e:Encounter {id: row.ENCOUNTER})
MATCH (p:Procedure {id: row.CODE})
MERGE (e)-[:HAS_PROCEDURE]->(p);

// Query to retrieve a specific patient's encounters and their related procedures and conditions
MATCH (p:Patient {id: "<patient_id>"})-[d:FIRST_ENCOUNTER]->(start:Encounter)
MATCH path = (start)-[:NEXT_ENCOUNTER*0..]->(e:Encounter)
OPTIONAL MATCH (e)-[q:HAS_PROCEDURE]-(pr:Procedure)
OPTIONAL MATCH (e)-[t:HAS_CONDITION]-(c:Condition)
RETURN p, e, pr, c, path,q,t,d
ORDER BY e.START;

// Query to retrieve all procedures for a specific patient and their encounter times
// This will return the procedure names and their corresponding encounter times for a specific patient
// Replace "<patient_id>" with the actual patient ID you want to query

MATCH (p:Patient {id: "<patient_id>"})-[:FIRST_ENCOUNTER]->(start:Encounter)
MATCH path = (start)-[:NEXT_ENCOUNTER*0..]->(e:Encounter)
MATCH (e)-[:HAS_PROCEDURE]->(pr:Procedure)
RETURN pr.description AS procedure_name, e.start AS encounter_time
ORDER BY e.start