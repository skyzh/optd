-- (no id or description)
create table t1(t1v1 int, t1v2 int);
create table t2(t2v1 int, t2v3 int);
create table t3(t3v2 int, t3v4 int);

/*

*/

-- Test whether the optimizer can unnest correlated subqueries with (scalar op agg)
select * from t1 where (select sum(t2v3) from t2 where t2v1 = t1v1) > 100;

/*
LogicalProjection { exprs: [ #0, #1 ] }
└── LogicalFilter
    ├── cond:Gt
    │   ├── #2
    │   └── 100(i64)
    └── DependentJoin { join_type: Inner, cond: true, extern_cols: [ Extern(#0) ] }
        ├── LogicalScan { table: t1 }
        └── LogicalProjection { exprs: [ #0 ] }
            └── LogicalAgg
                ├── exprs:Agg(Sum)
                │   └── [ Cast { cast_to: Int64, child: #1 } ]
                ├── groups: []
                └── LogicalFilter
                    ├── cond:Eq
                    │   ├── #0
                    │   └── Extern(#0)
                    └── LogicalScan { table: t2 }
LogicalProjection { exprs: [ #0, #1 ] }
└── LogicalFilter
    ├── cond:Gt
    │   ├── #2
    │   └── 100(i64)
    └── LogicalProjection { exprs: [ #0, #1, #2 ] }
        └── LogicalAgg
            ├── exprs:Agg(Sum)
            │   └── [ Cast { cast_to: Int64, child: #3 } ]
            ├── groups: [ #0, #1 ]
            └── LogicalFilter
                ├── cond:Eq
                │   ├── #2
                │   └── #0
                └── LogicalJoin { join_type: Inner, cond: true }
                    ├── LogicalScan { table: t1 }
                    └── LogicalScan { table: t2 }
PhysicalProjection { exprs: [ #0, #1 ], cost: {compute=15813.1,io=2030}, stat: {row_cnt=1} }
└── PhysicalFilter
    ├── cond:Gt
    │   ├── #2
    │   └── 100(i64)
    ├── cost: {compute=15810,io=2030}
    ├── stat: {row_cnt=1}
    └── PhysicalGather { cost: {compute=12710,io=2030}, stat: {row_cnt=1000} }
        └── PhysicalHashAgg
            ├── aggrs:Agg(Sum)
            │   └── [ Cast { cast_to: Int64, child: #3 } ]
            ├── groups: [ #0, #1 ]
            ├── cost: {compute=12610,io=2020}
            ├── stat: {row_cnt=1000}
            └── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ], cost: {compute=3420,io=2020}, stat: {row_cnt=1000} }
                ├── PhysicalHashShuffle { columns: [ #0 ], cost: {compute=110,io=1010}, stat: {row_cnt=1000} }
                │   └── PhysicalScan { table: t1, cost: {compute=0,io=1000}, stat: {row_cnt=1000} }
                └── PhysicalHashShuffle { columns: [ #0 ], cost: {compute=110,io=1010}, stat: {row_cnt=1000} }
                    └── PhysicalScan { table: t2, cost: {compute=0,io=1000}, stat: {row_cnt=1000} }
*/

-- Test whether the optimizer can unnest correlated subqueries with (scalar op group agg)
select * from t1 where (select sum(sumt2v3) from (select t2v1, sum(t2v3) as sumt2v3 from t2 where t2v1 = t1v1 group by t2v1)) > 100;

/*
LogicalProjection { exprs: [ #0, #1 ] }
└── LogicalFilter
    ├── cond:Gt
    │   ├── #2
    │   └── 100(i64)
    └── DependentJoin { join_type: Inner, cond: true, extern_cols: [ Extern(#0) ] }
        ├── LogicalScan { table: t1 }
        └── LogicalProjection { exprs: [ #0 ] }
            └── LogicalAgg
                ├── exprs:Agg(Sum)
                │   └── [ #1 ]
                ├── groups: []
                └── LogicalProjection { exprs: [ #0, #1 ] }
                    └── LogicalAgg
                        ├── exprs:Agg(Sum)
                        │   └── [ Cast { cast_to: Int64, child: #1 } ]
                        ├── groups: [ #0 ]
                        └── LogicalFilter
                            ├── cond:Eq
                            │   ├── #0
                            │   └── Extern(#0)
                            └── LogicalScan { table: t2 }
LogicalProjection { exprs: [ #0, #1 ] }
└── LogicalFilter
    ├── cond:Gt
    │   ├── #2
    │   └── 100(i64)
    └── LogicalProjection { exprs: [ #0, #1, #2 ] }
        └── LogicalAgg
            ├── exprs:Agg(Sum)
            │   └── [ #3 ]
            ├── groups: [ #0, #1 ]
            └── LogicalProjection { exprs: [ #0, #1, #2, #3 ] }
                └── LogicalAgg
                    ├── exprs:Agg(Sum)
                    │   └── [ Cast { cast_to: Int64, child: #3 } ]
                    ├── groups: [ #0, #1, #2 ]
                    └── LogicalFilter
                        ├── cond:Eq
                        │   ├── #2
                        │   └── #0
                        └── LogicalJoin { join_type: Inner, cond: true }
                            ├── LogicalScan { table: t1 }
                            └── LogicalScan { table: t2 }
PhysicalProjection { exprs: [ #0, #1 ], cost: {compute=24103.1,io=2040}, stat: {row_cnt=1} }
└── PhysicalFilter
    ├── cond:Gt
    │   ├── #2
    │   └── 100(i64)
    ├── cost: {compute=24100,io=2040}
    ├── stat: {row_cnt=1}
    └── PhysicalGather { cost: {compute=21000,io=2040}, stat: {row_cnt=1000} }
        └── PhysicalHashAgg
            ├── aggrs:Agg(Sum)
            │   └── [ #3 ]
            ├── groups: [ #0, #1 ]
            ├── cost: {compute=20900,io=2030}
            ├── stat: {row_cnt=1000}
            └── PhysicalHashShuffle { columns: [ #0, #1 ], cost: {compute=13730,io=2030}, stat: {row_cnt=1000} }
                └── PhysicalHashAgg
                    ├── aggrs:Agg(Sum)
                    │   └── [ Cast { cast_to: Int64, child: #3 } ]
                    ├── groups: [ #0, #1, #2 ]
                    ├── cost: {compute=13620,io=2020}
                    ├── stat: {row_cnt=1000}
                    └── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ], cost: {compute=3420,io=2020}, stat: {row_cnt=1000} }
                        ├── PhysicalHashShuffle { columns: [ #0 ], cost: {compute=110,io=1010}, stat: {row_cnt=1000} }
                        │   └── PhysicalScan { table: t1, cost: {compute=0,io=1000}, stat: {row_cnt=1000} }
                        └── PhysicalHashShuffle { columns: [ #0 ], cost: {compute=110,io=1010}, stat: {row_cnt=1000} }
                            └── PhysicalScan { table: t2, cost: {compute=0,io=1000}, stat: {row_cnt=1000} }
*/

-- Test whether the optimizer can unnest correlated subqueries with scalar agg in select list
select t1v1, (select sum(t2v3) from t2 where t2v1 = t1v1) as sum from t1;

/*
LogicalProjection { exprs: [ #0, #2 ] }
└── DependentJoin { join_type: Inner, cond: true, extern_cols: [ Extern(#0) ] }
    ├── LogicalScan { table: t1 }
    └── LogicalProjection { exprs: [ #0 ] }
        └── LogicalAgg
            ├── exprs:Agg(Sum)
            │   └── [ Cast { cast_to: Int64, child: #1 } ]
            ├── groups: []
            └── LogicalFilter
                ├── cond:Eq
                │   ├── #0
                │   └── Extern(#0)
                └── LogicalScan { table: t2 }
LogicalProjection { exprs: [ #0, #2 ] }
└── LogicalProjection { exprs: [ #0, #1, #2 ] }
    └── LogicalAgg
        ├── exprs:Agg(Sum)
        │   └── [ Cast { cast_to: Int64, child: #3 } ]
        ├── groups: [ #0, #1 ]
        └── LogicalFilter
            ├── cond:Eq
            │   ├── #2
            │   └── #0
            └── LogicalJoin { join_type: Inner, cond: true }
                ├── LogicalScan { table: t1 }
                └── LogicalScan { table: t2 }
PhysicalProjection { exprs: [ #0, #2 ], cost: {compute=15810,io=2030}, stat: {row_cnt=1000} }
└── PhysicalGather { cost: {compute=12710,io=2030}, stat: {row_cnt=1000} }
    └── PhysicalHashAgg
        ├── aggrs:Agg(Sum)
        │   └── [ Cast { cast_to: Int64, child: #3 } ]
        ├── groups: [ #0, #1 ]
        ├── cost: {compute=12610,io=2020}
        ├── stat: {row_cnt=1000}
        └── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ], cost: {compute=3420,io=2020}, stat: {row_cnt=1000} }
            ├── PhysicalHashShuffle { columns: [ #0 ], cost: {compute=110,io=1010}, stat: {row_cnt=1000} }
            │   └── PhysicalScan { table: t1, cost: {compute=0,io=1000}, stat: {row_cnt=1000} }
            └── PhysicalHashShuffle { columns: [ #0 ], cost: {compute=110,io=1010}, stat: {row_cnt=1000} }
                └── PhysicalScan { table: t2, cost: {compute=0,io=1000}, stat: {row_cnt=1000} }
*/

-- Test whether the optimizer can unnest correlated subqueries.
select * from t1 where (select sum(t2v3) from (select * from t2, t3 where t2v1 = t1v1 and t2v3 = t3v2)) > 100;

/*
LogicalProjection { exprs: [ #0, #1 ] }
└── LogicalFilter
    ├── cond:Gt
    │   ├── #2
    │   └── 100(i64)
    └── DependentJoin { join_type: Inner, cond: true, extern_cols: [ Extern(#0) ] }
        ├── LogicalScan { table: t1 }
        └── LogicalProjection { exprs: [ #0 ] }
            └── LogicalAgg
                ├── exprs:Agg(Sum)
                │   └── [ Cast { cast_to: Int64, child: #1 } ]
                ├── groups: []
                └── LogicalProjection { exprs: [ #0, #1, #2, #3 ] }
                    └── LogicalFilter
                        ├── cond:And
                        │   ├── Eq
                        │   │   ├── #0
                        │   │   └── Extern(#0)
                        │   └── Eq
                        │       ├── #1
                        │       └── #2
                        └── LogicalJoin { join_type: Inner, cond: true }
                            ├── LogicalScan { table: t2 }
                            └── LogicalScan { table: t3 }
LogicalProjection { exprs: [ #0, #1 ] }
└── LogicalFilter
    ├── cond:Gt
    │   ├── #2
    │   └── 100(i64)
    └── LogicalProjection { exprs: [ #0, #1, #2 ] }
        └── LogicalAgg
            ├── exprs:Agg(Sum)
            │   └── [ Cast { cast_to: Int64, child: #3 } ]
            ├── groups: [ #0, #1 ]
            └── LogicalProjection { exprs: [ #0, #1, #2, #3, #4, #5 ] }
                └── LogicalFilter
                    ├── cond:And
                    │   ├── Eq
                    │   │   ├── #2
                    │   │   └── #0
                    │   └── Eq
                    │       ├── #3
                    │       └── #4
                    └── LogicalJoin { join_type: Inner, cond: true }
                        ├── LogicalScan { table: t1 }
                        └── LogicalJoin { join_type: Inner, cond: true }
                            ├── LogicalScan { table: t2 }
                            └── LogicalScan { table: t3 }
PhysicalProjection { exprs: [ #0, #1 ], cost: {compute=19343.1,io=3060}, stat: {row_cnt=1} }
└── PhysicalFilter
    ├── cond:Gt
    │   ├── #2
    │   └── 100(i64)
    ├── cost: {compute=19340,io=3060}
    ├── stat: {row_cnt=1}
    └── PhysicalGather { cost: {compute=16240,io=3060}, stat: {row_cnt=1000} }
        └── PhysicalHashAgg
            ├── aggrs:Agg(Sum)
            │   └── [ Cast { cast_to: Int64, child: #3 } ]
            ├── groups: [ #0, #1 ]
            ├── cost: {compute=16140,io=3050}
            ├── stat: {row_cnt=1000}
            └── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ], cost: {compute=6840,io=3040}, stat: {row_cnt=1000} }
                ├── PhysicalHashShuffle { columns: [ #0 ], cost: {compute=110,io=1010}, stat: {row_cnt=1000} }
                │   └── PhysicalScan { table: t1, cost: {compute=0,io=1000}, stat: {row_cnt=1000} }
                └── PhysicalHashShuffle { columns: [ #0 ], cost: {compute=3530,io=2030}, stat: {row_cnt=1000} }
                    └── PhysicalHashJoin { join_type: Inner, left_keys: [ #1 ], right_keys: [ #0 ], cost: {compute=3420,io=2020}, stat: {row_cnt=1000} }
                        ├── PhysicalHashShuffle { columns: [ #1 ], cost: {compute=110,io=1010}, stat: {row_cnt=1000} }
                        │   └── PhysicalScan { table: t2, cost: {compute=0,io=1000}, stat: {row_cnt=1000} }
                        └── PhysicalHashShuffle { columns: [ #0 ], cost: {compute=110,io=1010}, stat: {row_cnt=1000} }
                            └── PhysicalScan { table: t3, cost: {compute=0,io=1000}, stat: {row_cnt=1000} }
*/

