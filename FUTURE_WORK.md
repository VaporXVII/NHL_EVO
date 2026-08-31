# Overview 

This section outlines planned enhancements to NHL EVO.

## August 2026 

##### Lakeflow Declarative Pipelines

Implement **Databricks Lakeflow Declarative Pipelines (LDP)** with streaming **AUTO CDC** and **Amazon S3 external volume integration** for the Play-by-Play (PBP) and Shift data endpoints.

The existing batch architecture has served NHL EVO well. However, because the PBP and Shift endpoints are continuously queried during live games, a streaming architecture using Lakeflow Declarative Pipelines, S3, and AUTO CDC may provide a more efficient and scalable approach for ingesting and processing frequently changing game data.

This feature has completed the development phase and is awaiting live-game testing when the upcoming NHL season begins. Testing will determine whether the architecture can operate effectively within the compute and streaming limitations of Databricks Free Edition without compromising existing job orchestrations. 


##### Databricks Asset Bundles (DAB)

Incorporating Databricks Asset bundles to appropriately manage jobs orchestration as well as upcoming DLDP architecture. 

##### Play-by-Play Shift Game Level Assets 

At the end of the 2026 season, a new asset was created for all historical games that combines both the PBP and Shift data assets into one, allowing users to see the chronology of shifts and plays within each game. This enhancement has already passed the development phase and is awaiting live-game testing when the upcoming NHL season begins. Testing will determine whether the architecture can operate effectively within the compute limitations of Databricks Free Edition. 