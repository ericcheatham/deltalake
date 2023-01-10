
# Meroxa Postgres -> Delta Table

## Architecture 

Currently:
 PG (source) -> Function ( Processes data ) -> PG (destination)


## Library
Uses [delta-rs](https://github.com/delta-io/delta-rs) which provides an interface with various delta lake providers. 

