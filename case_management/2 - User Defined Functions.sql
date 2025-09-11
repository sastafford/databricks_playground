-- Databricks notebook source
USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA IDENTIFIER(:schema);

-- COMMAND ----------

DROP FUNCTION IF EXISTS high_clearance;
DROP FUNCTION IF EXISTS medium_clearance;
DROP FUNCTION IF EXISTS low_clearance;
DROP FUNCTION IF EXISTS a_compartment;
DROP FUNCTION IF EXISTS b_compartment;
DROP FUNCTION IF EXISTS c_compartment;
DROP FUNCTION IF EXISTS d_compartment;
DROP FUNCTION IF EXISTS e_compartment;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION high_clearance(clearance STRING)
RETURNS BOOLEAN
RETURN
  contains(clearance, "HIGH") OR contains(clearance, "MEDIUM") OR contains(clearance, "LOW");

CREATE OR REPLACE FUNCTION medium_clearance(clearance STRING)
RETURNS BOOLEAN
RETURN
  contains(clearance, "MEDIUM") OR contains(clearance, "LOW");

CREATE OR REPLACE FUNCTION low_clearance(clearance STRING)
RETURNS BOOLEAN
RETURN
  contains(clearance, "LOW");


-- COMMAND ----------

CREATE OR REPLACE FUNCTION a_compartment(compartment STRING)
RETURNS BOOLEAN
RETURN
  contains(compartment, "A");

CREATE OR REPLACE FUNCTION b_compartment(compartment STRING)
RETURNS BOOLEAN
RETURN
  contains(compartment, "B");

CREATE OR REPLACE FUNCTION c_compartment(compartment STRING)
RETURNS BOOLEAN
RETURN
  contains(compartment, "C");

CREATE OR REPLACE FUNCTION d_compartment(compartment STRING)
RETURNS BOOLEAN
RETURN
  contains(compartment, "D");

CREATE OR REPLACE FUNCTION e_compartment(compartment STRING)
RETURNS BOOLEAN
RETURN
  contains(compartment, "E");

-- COMMAND ----------

SELECT *
FROM cases
LIMIT 100
