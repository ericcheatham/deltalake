#!/usr/bin/python

import os

import psycopg
from faker import Faker

# Connect to the PostgreSQL database
conn = psycopg.connect(conninfo=os.getenv("POSTGRES_CONN_URL"))

# Create a cursor object to execute SQL commands
cursor = conn.cursor()

# Create an instance of the Faker class
faker = Faker()

# Create the 'employees' table if it does not exist
cursor.execute(
    """CREATE TABLE IF NOT EXISTS employees (
	    id SERIAL PRIMARY KEY,
	    first_name CHARACTER VARYING (100),
        last_name CHARACTER VARYING (25) NOT NULL,
        job_title_id INTEGER NOT NULL,
        department_id INTEGER NOT NULL,
        gender_id INTEGER NOT NULL,
        address CHARACTER VARYING (100) NOT NULL,
        postcode CHARACTER VARYING (10) NOT NULL,
        city_id INTEGER NOT NULL,
        email CHARACTER VARYING (100) NOT NULL,
        employment_start DATE NOT NULL
    )"""
)

# Use a loop to generate multiple records for employees table
for _ in range(20):
    # Use the Faker class to generate data
    first_name = faker.first_name()
    last_name = faker.last_name()
    job_title_id = faker.random_int(min=1, max=5)
    department_id = faker.random_int(min=1, max=5)
    gender_id = faker.random_int(min=1, max=2)
    address = faker.address()
    postcode = faker.postcode()
    city_id = faker.random_int(min=1, max=5)
    email = faker.email()
    employment_start = faker.date()

    # Execute an SQL INSERT command, passing in the generated data as values
    cursor.execute(
        f"INSERT INTO employees (first_name, last_name, job_title_id, department_id, gender_id, address, postcode, city_id, email, employment_start) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (
            first_name,
            last_name,
            job_title_id,
            department_id,
            gender_id,
            address,
            postcode,
            city_id,
            email,
            employment_start,
        ),
    )

# Commit the changes to the database
conn.commit()
