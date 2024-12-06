-- (no id or description)
create table t1(v1 int, v2 int, v3 int);
create table t2(v4 int);
create table t3(v5 int, v6 int);
insert into t1 values (0, 200, 300), (3, 201, 301), (2, 202, 302);

/*
3
*/

-- test shuffle passthrough, should generate two shuffles
select v1, sum(v2), sum(sum_v3) from (select v1, v2, sum(v3) as sum_v3 from t1 group by v1, v2) group by v1;

/*
LogicalProjection { exprs: [ #0, #1, #2 ] }
└── LogicalAgg
    ├── exprs:
    │   ┌── Agg(Sum)
    │   │   └── [ Cast { cast_to: Int64, child: #1 } ]
    │   └── Agg(Sum)
    │       └── [ #2 ]
    ├── groups: [ #0 ]
    └── LogicalProjection { exprs: [ #0, #1, #2 ] }
        └── LogicalAgg
            ├── exprs:Agg(Sum)
            │   └── [ Cast { cast_to: Int64, child: #2 } ]
            ├── groups: [ #0, #1 ]
            └── LogicalScan { table: t1 }
PhysicalGather
└── PhysicalHashAgg
    ├── aggrs:
    │   ┌── Agg(Sum)
    │   │   └── [ Cast { cast_to: Int64, child: #1 } ]
    │   └── Agg(Sum)
    │       └── [ #2 ]
    ├── groups: [ #0 ]
    └── PhysicalHashShuffle { columns: [ #0 ] }
        └── PhysicalHashAgg
            ├── aggrs:Agg(Sum)
            │   └── [ Cast { cast_to: Int64, child: #2 } ]
            ├── groups: [ #0, #1 ]
            └── PhysicalHashShuffle { columns: [ #0, #1 ] }
                └── PhysicalScan { table: t1 }
*/

-- test shuffle passthrough, should generate two shuffles
select v2, sum(v1), sum(sum_v3) from (select v1, v2, sum(v3) as sum_v3 from t1 group by v1, v2) group by v2;

/*
LogicalProjection { exprs: [ #0, #1, #2 ] }
└── LogicalAgg
    ├── exprs:
    │   ┌── Agg(Sum)
    │   │   └── [ Cast { cast_to: Int64, child: #0 } ]
    │   └── Agg(Sum)
    │       └── [ #2 ]
    ├── groups: [ #1 ]
    └── LogicalProjection { exprs: [ #0, #1, #2 ] }
        └── LogicalAgg
            ├── exprs:Agg(Sum)
            │   └── [ Cast { cast_to: Int64, child: #2 } ]
            ├── groups: [ #0, #1 ]
            └── LogicalScan { table: t1 }
PhysicalGather
└── PhysicalHashAgg
    ├── aggrs:
    │   ┌── Agg(Sum)
    │   │   └── [ Cast { cast_to: Int64, child: #0 } ]
    │   └── Agg(Sum)
    │       └── [ #2 ]
    ├── groups: [ #1 ]
    └── PhysicalHashShuffle { columns: [ #1 ] }
        └── PhysicalHashAgg
            ├── aggrs:Agg(Sum)
            │   └── [ Cast { cast_to: Int64, child: #2 } ]
            ├── groups: [ #0, #1 ]
            └── PhysicalHashShuffle { columns: [ #0, #1 ] }
                └── PhysicalScan { table: t1 }
*/

-- test shuffle passthrough, should generate one shuffle because shuffle by v1 satisfies shuffle by v1, v2
select * from t1, t2, t3 where v1 = v4 and v1 = v5 and v2 = v6;

/*
LogicalProjection { exprs: [ #0, #1, #2, #3, #4, #5 ] }
└── LogicalFilter
    ├── cond:And
    │   ├── Eq
    │   │   ├── #0
    │   │   └── #3
    │   ├── Eq
    │   │   ├── #0
    │   │   └── #4
    │   └── Eq
    │       ├── #1
    │       └── #5
    └── LogicalJoin { join_type: Inner, cond: true }
        ├── LogicalJoin { join_type: Inner, cond: true }
        │   ├── LogicalScan { table: t1 }
        │   └── LogicalScan { table: t2 }
        └── LogicalScan { table: t3 }
PhysicalGather
└── PhysicalHashJoin { join_type: Inner, left_keys: [ #0, #1 ], right_keys: [ #0, #1 ] }
    ├── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ] }
    │   ├── PhysicalHashShuffle { columns: [ #0 ] }
    │   │   └── PhysicalScan { table: t1 }
    │   └── PhysicalHashShuffle { columns: [ #0 ] }
    │       └── PhysicalScan { table: t2 }
    └── PhysicalHashShuffle { columns: [ #0, #1 ] }
        └── PhysicalScan { table: t3 }
*/

