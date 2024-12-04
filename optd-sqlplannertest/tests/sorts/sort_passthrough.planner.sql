-- (no id or description)
create table t1(t1v1 int, t1v2 int);
insert into t1 values (0, 200), (3, 201), (2, 202);

/*
3
*/

-- test order by passthrough
select * from (select * from (select * from (select * from t1 order by t1v1) order by t1v1) order by t1v1) order by t1v1;

/*
LogicalSort
├── exprs:SortOrder { order: Asc }
│   └── #0
└── LogicalProjection { exprs: [ #0, #1 ] }
    └── LogicalProjection { exprs: [ #0, #1 ] }
        └── LogicalProjection { exprs: [ #0, #1 ] }
            └── LogicalProjection { exprs: [ #0, #1 ] }
                └── LogicalScan { table: t1 }
PhysicalSort
├── exprs:SortOrder { order: Asc }
│   └── #0
└── PhysicalScan { table: t1 }
0 200
2 202
3 201
group_id=!2
  subgoal_id=.10 winner=16 weighted_cost=1000 | (PhysicalScan P0)
    cost={compute=0,io=1000}
    stat={row_cnt=1000}
    (required) sort=<any>
    (derived) sort=<any>
  subgoal_id=.11 winner=16 weighted_cost=1000 | (PhysicalScan P0)
    cost={compute=0,io=1000}
    stat={row_cnt=1000}
    (required) sort=<any>
    (derived) sort=<any>
  subgoal_id=.19 winner=(Enforcer)14 weighted_cost=8008.754779315221 | (PhysicalSort !2 P6) goal=.11
    cost={compute=7008.754779315221,io=1000}
    stat={row_cnt=1000}
    (required) sort=[Asc#0]
    (derived) sort=[Asc#0]
  schema=[t1v1:Int32, t1v2:Int32]
  column_ref=[t1.0, t1.1]
  expr_id=1 | (Scan P0)
  expr_id=4 | (Projection !2 P3)
  expr_id=16 | (PhysicalScan P0)
  expr_id=18 | (PhysicalProjection !2 P3)
  P0=(Constant(Utf8String) "t1")
  P3=(List (ColumnRef 0(u64)) (ColumnRef 1(u64)))
group_id=!8
  subgoal_id=.9 winner=14 weighted_cost=8008.754779315221 | (PhysicalSort !2 P6)
    cost={compute=7008.754779315221,io=1000}
    stat={row_cnt=1000}
    (required) sort=<any>
    (derived) sort=[Asc#0]
  schema=[unnamed:UInt64, unnamed:UInt64]
  column_ref=[t1.0, t1.1]
  expr_id=7 | (Sort !2 P6)
  expr_id=14 | (PhysicalSort !2 P6)
  P6=(List (SortOrder(Asc) (ColumnRef 0(u64))))
*/

-- test order by passthrough
select * from (select * from t1 order by t1v1) order by t1v1;

/*
LogicalSort
├── exprs:SortOrder { order: Asc }
│   └── #0
└── LogicalProjection { exprs: [ #0, #1 ] }
    └── LogicalProjection { exprs: [ #0, #1 ] }
        └── LogicalScan { table: t1 }
PhysicalSort
├── exprs:SortOrder { order: Asc }
│   └── #0
└── PhysicalScan { table: t1 }
0 200
2 202
3 201
*/

-- test order by passthrough
select * from (select * from t1 order by t1v1, t1v2) order by t1v1;

/*
LogicalSort
├── exprs:SortOrder { order: Asc }
│   └── #0
└── LogicalProjection { exprs: [ #0, #1 ] }
    └── LogicalProjection { exprs: [ #0, #1 ] }
        └── LogicalScan { table: t1 }
PhysicalSort
├── exprs:SortOrder { order: Asc }
│   └── #0
└── PhysicalScan { table: t1 }
0 200
2 202
3 201
*/

-- test order by passthrough
select * from (select * from t1 order by t1v1) order by t1v1, t1v2;

/*
LogicalSort
├── exprs:
│   ┌── SortOrder { order: Asc }
│   │   └── #0
│   └── SortOrder { order: Asc }
│       └── #1
└── LogicalProjection { exprs: [ #0, #1 ] }
    └── LogicalProjection { exprs: [ #0, #1 ] }
        └── LogicalScan { table: t1 }
PhysicalSort
├── exprs:
│   ┌── SortOrder { order: Asc }
│   │   └── #0
│   └── SortOrder { order: Asc }
│       └── #1
└── PhysicalScan { table: t1 }
0 200
2 202
3 201
*/

