-- (no id or description)
create table t1(t1v1 int, t1v2 int);
create table t2(t2v1 int, t2v3 int);
insert into t1 values (0, 0), (1, 1), (2, 2);
insert into t2 values (0, 200), (1, 201), (2, 202);

/*
3
3
*/

-- test self join
select * from t1 as a, t1 as b where a.t1v1 = b.t1v1 order by a.t1v1;

/*
(Join t1 t1)

LogicalSort
├── exprs:SortOrder { order: Asc }
│   └── #0
└── LogicalProjection { exprs: [ #0, #1, #2, #3 ] }
    └── LogicalFilter
        ├── cond:Eq
        │   ├── #0
        │   └── #2
        └── LogicalJoin { join_type: Inner, cond: true }
            ├── LogicalScan { table: t1 }
            └── LogicalScan { table: t1 }
PhysicalSort
├── exprs:SortOrder { order: Asc }
│   └── #0
└── PhysicalGather
    └── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ] }
        ├── PhysicalHashShuffle { columns: [ #0 ] }
        │   └── PhysicalScan { table: t1 }
        └── PhysicalHashShuffle { columns: [ #0 ] }
            └── PhysicalScan { table: t1 }
0 0 0 0
1 1 1 1
2 2 2 2
*/

