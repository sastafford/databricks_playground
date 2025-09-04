CREATE TEMPORARY VIEW person_snapshot AS 
  SELECT *
  FROM bronze_person
  WHERE snapshot = (SELECT max(snapshot) FROM bronze_person)