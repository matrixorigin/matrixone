# MatrixOne 目录结构

完成 MatrixOne 搭建和连接后，首次执行时，MatrixOne 会自动生成以下目录，用于存放各类数据文件或元数据信息。

进入 *matrixone* 目录执行 `ls` 查看目录结构，相关目录结构以及用途如下：

matrixone    //MatrixOne主目录<br>
├── etc   //配置文件目录<br>
│   ├── launch-tae-CN-tae-DN  //分布式配置文件目录<br>
│   ├── launch-tae-logservice  //单机版配置文件目录<br>
├── mo-data  //数据文件目录<br>
│   ├── local   //本地fileservice目录<br>
│   │   ├── cn //本地cn与存储目录<br>
│   │   └── etl   //统计信息目录<br>
│   │       └── sys //统计信息归属于哪个account<br>
│   │           └── logs //统计信息的类型<br>
│   │               └── 2022 //统计信息的年份<br>
│   │                   └── 10  //统计信息的月份<br>
│   │                       └── 27 //统计信息的天数<br>
│   │                           ├── metric //统计信息对应表的存储目录<br>
│   │                           ├── rawlog //统计信息对应表的存储目录<br>
│   │                           └── statement_info //统计信息对应表的存储路目录<br>
│   └── logservice  //logservice目录<br>
│       ├──hostname //MatrixOne的服务器域名<br>
│       │   └── 00000000000000000001 //快照保存目录<br>
│       │       └── tandb //bootstrap信息保存目录<br>
└── store //tae的catalog保存目录<br>
    └── tae<br>
