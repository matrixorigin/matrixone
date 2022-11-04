# MatrixOne Directory Structure

After MatrixOne has been installed and connected, the MatrixOne automatically generates the following directories for storing data files or metadata information.

Enter into the *matrixone* directory and execute `ls` to view the directory structure. The related directory structure and uses are as follows:

matrixone    //MatrixOne main directory<br>
├── etc   //configuration file directory<br>
│   ├── launch-tae-CN-tae-DN  //distributed configuration file directory<br>
│   ├── launch-tae-logservice  //standalone configuration file directory<br>
├── mo-data  //data file directory<br>
│   ├── local   //local fileservice directory<br>
│   │   ├── cn //local cn and storage directory<br>
│   │   └── etl   //statistics directory<br>
│   │       └── sys //statistics belongs to the account<br>
│   │           └── logs //the type of statistics<br>
│   │               └── 2022 // the year of statistics<br>
│   │                   └── 10  //month of statistics<br>
│   │                       └── 27 // the number of days of statistics<br>
│   │                           ├── metric //system indicators storage directory<br>
│   │                           ├── rawlog //log storage directory<br>
│   │                           └── statement_info //sql information storage directory<br>
│   └── logservice  //logservice directory<br>
│       ├──hostname //MatrixOne server domain name<br>
│       │   └── 00000000000000000001 //snapshot directory<br>
│       │       └── tandb //bootstrap directory<br>
└── store //tae catalog directory<br>
    └── tae<br>
