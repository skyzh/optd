-- (no id or description)
create table t1(v1 int);
insert into t1 values (0), (1), (2), (3);

/*
4
*/

-- Test non-verbose explain
select * from t1;

/*
PhysicalGather
└── PhysicalScan { table: t1 }
*/

-- Test verbose explain
select * from t1;

/*
PhysicalGather { cost: {compute=100,io=1010}, stat: {row_cnt=1000} }
└── PhysicalScan { table: t1, cost: {compute=0,io=1000}, stat: {row_cnt=1000} }
*/

-- Test verbose explain with aggregation
select count(*) from t1;

/*
PhysicalStreamAgg
├── aggrs:Agg(Count)
│   └── [ 1(i64) ]
├── groups: []
├── cost: {compute=5200,io=1010}
├── stat: {row_cnt=1000}
└── PhysicalGather { cost: {compute=100,io=1010}, stat: {row_cnt=1000} }
    └── PhysicalScan { table: t1, cost: {compute=0,io=1000}, stat: {row_cnt=1000} }
*/

