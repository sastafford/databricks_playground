# LakeFlow Playground

These are different examples of how to use various aspects of LakeFlow

## CDC Snapshot

A CDC snapshot in Databricks refers to a mechanism for implementing Change Data Capture (CDC) using periodic "snapshots" of source tables or databases. Instead of relying solely on transaction logs or database change data feeds, CDC from snapshots compares a series of full snapshots (i.e., complete copies of the source data at different points in time) to detect and process inserts, updates, and deletes.

References
 - https://www.databricks.com/blog/how-perform-change-data-capture-cdc-full-table-snapshots-using-delta-live-tables
 - https://docs.databricks.com/aws/en/dlt/cdc

### Setup the demo

```
databricks bundle deploy --var="catalog=main"
```
### Run the demo notebook and follow the instructions

Run the [demo](./demo.ipynb) notebook.  

### Teardown the resources used for the demo

```
databricks bundle destroy --var="catalog=main"
```
## Troubleshooting

```
databricks bundle sync --full 
```
