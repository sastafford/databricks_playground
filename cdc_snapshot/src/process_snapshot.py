import dlt

dlt.create_streaming_table("silver_person_type_1")
dlt.create_streaming_table("silver_person_type_2")

dlt.create_auto_cdc_from_snapshot_flow(
 target="silver_person_type_2",
 source="person_snapshot",
 keys=["id"],
 stored_as_scd_type=2
)

dlt.create_auto_cdc_from_snapshot_flow(
 target="silver_person_type_1",
 source="person_snapshot",
 keys=["id"],
 stored_as_scd_type=1
)