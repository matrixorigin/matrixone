# MatrixOne 目录结构

完成 MatrixOne 搭建和连接后，首次执行时，MatrixOne 会自动生成以下目录，用于存放各类数据文件或元数据信息。

进入 *matrixone* 目录执行 `ls` 查看目录结构，相关目录结构以及用途如下：

matrixone    //MatrixOne主目录
├── etc   //配置文件目录
│   ├── launch-tae-CN-tae-DN  //分布式配置文件目录
│   ├── launch-tae-logservice  //单机版配置文件目录
├── mo-data  //数据文件目录
│   ├── local   //本地fileservice目录
│   │   ├── cn //本地cn与存储目录
│   │   └── etl   //统计信息目录
│   │       └── sys //统计信息归属于哪个account
│   │           └── logs //统计信息的类型
│   │               └── 2022 //统计信息的年份
│   │                   └── 10  //统计信息的月份
│   │                       └── 27 //统计信息的天数
│   │                           ├── metric //统计信息对应表的存储目录
│   │                           ├── rawlog //统计信息对应表的存储目录
│   │                           └── statement_info //统计信息对应表的存储路目录
│   └── logservice  //logservice目录
│       ├──hostname //MatrixOne的服务器域名
│       │   └── 00000000000000000001 //快照保存目录
│       │       └── tandb //bootstrap信息保存目录
└── store //tae的catalog保存目录
    └── tae
