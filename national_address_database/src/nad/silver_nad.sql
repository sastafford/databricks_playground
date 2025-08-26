CREATE OR REPLACE STREAMING TABLE silver_national_address(
    CONSTRAINT update_date_greater_than_1997 EXPECT (year(update_date) >= 1997)
) AS
    SELECT
        INT(OID_) as id,
        Add_Number as address_number,
        AddNo_Full as complete_address_number,
        St_Name as street_name,
        StNam_Full as complete_street_name,
        County as county,
        Inc_Muni as incorporated_municipality,
        Post_City as city,
        State as state,
        concat("US-", State) as country_state,
        Zip_Code as zip_code,
        Addr_Type as address_type,
        AddrClass as address_class,
        Nbrhd_Comm as neighborhood_community,
        NatGrid as national_grid,
        Placement as placement,
        latitude,
        longitude,
        AddrPoint as address_point,
        to_timestamp(Effective, 'M/d/yyyy H:mm:ss') as effective_date, 
        to_timestamp(Expire, 'M/d/yyyy H:mm:ss') as expiration_date,
        to_timestamp(DateUpdate, 'M/d/yyyy H:mm:ss') as update_date,
        NAD_Source as nad_data_provider,
        "US" as country
    FROM STREAM(bronze_national_address)
