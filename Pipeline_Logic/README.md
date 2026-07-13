# Overview 
Section contains notebooks that are critical to ensuring pipeline runs properly, based on the logic below:
- If pipeline has never been ran before -> run entire pipeline end-to-end
- If pipeline has been run before, check to see if there are any games scheduled for the current date
    - if yes: run pipeline to collect information ahead of the current date's scheduled games (team rosters, updating new and existing player identification numbers if necessary) as well as capturing data from any games that have been played
      in the prior two days (play-by-play and shift data)
    - if no: check to see if any games have been played in the prior two days
        - if yes: run pipeline to capture data that for games for the prior two days (play-by-play and shift data)
        - if no: skip 

## Notebook logic flow

```text
NHL_Data_Pipeline_Tables_Exist_Check
├── if True:
│   └── NHL_Data_Pipeline_Games_Check
│       ├── if games today = True:
│       │   └── run entire pipeline
│       └── if games today = False:
│           └── NHL_Data_Pipeline_Games_Check
│               └── if games yesterday = True:
│                   └── run notebooks to collect only play-by-play and shift data
│                       for games played from the prior two days
│
└── if False:
    └── NHL_Pipeline_Table_Creation_SQL
        └── if True:
            └── NHL_Data_Pipeline_First_Run
                ├── if True:
                │   └── run full pipeline logic (cold start)
                └── if False:
                    └── NHL_Data_Pipeline_Skip_Run
```
