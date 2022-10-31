# MatrixOne Directory Structure

After MatrixOne has been installed and connected, the MatrixOne automatically generates the following directories for storing data files or metadata information.

Enter into the *matrixone* directory and execute `ls` to view the directory structure. The related directory structure and uses are as follows:

matrixone    //MatrixOne main directory

├── etc   //configuration file directory

│   ├── launch-tae-CN-tae-DN  //distributed configuration file directory

│   ├── launch-tae-logservice  //standalone configuration file directory

├── mo-data  //data file directory

│   ├── local   //local fileservice directory

│   │   ├── cn //local cn and storage directory

│   │   └── etl   //statistics directory

│   │       └── sys //statistics belongs to the account

│   │           └── logs //the type of statistics

│   │               └── 2022 // the year of statistics

│   │                   └── 10  //month of statistics

│   │                       └── 27 // the number of days of statistics

│   │                           ├── metric //system indicators storage directory

│   │                           ├── rawlog //log storage directory

│   │                           └── statement_info //sql information storage directory

│   └── logservice  //logservice directory

│       ├──hostname //MatrixOne server domain name

│       │   └── 00000000000000000001 //snapshot directory

│       │       └── tandb //bootstrap directory

└── store //tae catalog directory

    └── tae
