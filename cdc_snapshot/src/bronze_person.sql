CREATE OR REFRESH STREAMING TABLE bronze_person
AS 
    SELECT * 
    FROM STREAM read_files(
        "/Volumes/usa/scott_stafford_cdc_snapshot/temp",
        format => "csv"
    );