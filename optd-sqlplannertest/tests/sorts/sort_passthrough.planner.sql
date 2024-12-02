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
    └── LogicalSort
        ├── exprs:SortOrder { order: Asc }
        │   └── #0
        └── LogicalProjection { exprs: [ #0, #1 ] }
            └── LogicalSort
                ├── exprs:SortOrder { order: Asc }
                │   └── #0
                └── LogicalProjection { exprs: [ #0, #1 ] }
                    └── LogicalSort
                        ├── exprs:SortOrder { order: Asc }
                        │   └── #0
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
  subgoal_id=.19 winner=(Enforcer)24 weighted_cost=8008.754779315221 | (PhysicalSort !2 P3) goal=.22
    cost={compute=7008.754779315221,io=1000}
    stat={row_cnt=1000}
    (required) sort=[Asc#0]
    (derived) sort=[Asc#0]
  subgoal_id=.22 winner=21 weighted_cost=1000 | (PhysicalScan P0)
    cost={compute=0,io=1000}
    stat={row_cnt=1000}
    (required) sort=<any>
    (derived) sort=<any>
  schema=[t1v1:Int32, t1v2:Int32]
  column_ref=[t1.0, t1.1]
  expr_id=1 | (Scan P0)
  expr_id=21 | (PhysicalScan P0)
  P0=(Constant(Utf8String) "t1")
group_id=!5
  subgoal_id=.18 winner=!2.19 weighted_cost=8008.754779315221 | !2.19
    cost={compute=7008.754779315221,io=1000}
    stat={row_cnt=1000}
    (required) sort=<any>
    (derived) sort=[Asc#0]
  subgoal_id=.28 winner=!2.19 weighted_cost=8008.754779315221 | !2.19
    cost={compute=7008.754779315221,io=1000}
    stat={row_cnt=1000}
    (required) sort=[Asc#0]
    (derived) sort=[Asc#0]
  schema=[t1v1:Int32, t1v2:Int32]
  column_ref=[t1.0, t1.1]
  expr_id=4 | (Sort !2 P3)
  expr_id=24 | (PhysicalSort !2 P3)
  P3=(List (SortOrder(Asc) (ColumnRef 0(u64))))
group_id=!8
  subgoal_id=.17 winner=!5.28 weighted_cost=8008.754779315221 | !5.28
    cost={compute=7008.754779315221,io=1000}
    stat={row_cnt=1000}
    (required) sort=[Asc#0]
    (derived) sort=[Asc#0]
  subgoal_id=.33 winner=!5.28 weighted_cost=8008.754779315221 | !5.28
    cost={compute=7008.754779315221,io=1000}
    stat={row_cnt=1000}
    (required) sort=<any>
    (derived) sort=[Asc#0]
  schema=[t1v1:Int32, t1v2:Int32]
  column_ref=[t1.0, t1.1]
  expr_id=7 | (Sort !5 P3)
  expr_id=27 | (PhysicalSort !5 P3)
  P3=(List (SortOrder(Asc) (ColumnRef 0(u64))))
group_id=!11
  subgoal_id=.16 winner=!8.17 weighted_cost=8008.754779315221 | !8.17
    cost={compute=7008.754779315221,io=1000}
    stat={row_cnt=1000}
    (required) sort=<any>
    (derived) sort=[Asc#0]
  subgoal_id=.32 winner=!8.17 weighted_cost=8008.754779315221 | !8.17
    cost={compute=7008.754779315221,io=1000}
    stat={row_cnt=1000}
    (required) sort=[Asc#0]
    (derived) sort=[Asc#0]
  schema=[t1v1:Int32, t1v2:Int32]
  column_ref=[t1.0, t1.1]
  expr_id=10 | (Sort !8 P3)
  expr_id=35 | (PhysicalSort !8 P3)
  P3=(List (SortOrder(Asc) (ColumnRef 0(u64))))
group_id=!14
  subgoal_id=.15 winner=!11.32 weighted_cost=8008.754779315221 | !11.32
    cost={compute=7008.754779315221,io=1000}
    stat={row_cnt=1000}
    (required) sort=<any>
    (derived) sort=[Asc#0]
  schema=[t1v1:Int32, t1v2:Int32]
  column_ref=[t1.0, t1.1]
  expr_id=13 | (Sort !11 P3)
  expr_id=31 | (PhysicalSort !11 P3)
  P3=(List (SortOrder(Asc) (ColumnRef 0(u64))))
*/

-- test order by passthrough
select * from (select * from t1 order by t1v1) order by t1v1;

/*
LogicalSort
├── exprs:SortOrder { order: Asc }
│   └── #0
└── LogicalProjection { exprs: [ #0, #1 ] }
    └── LogicalSort
        ├── exprs:SortOrder { order: Asc }
        │   └── #0
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
    └── LogicalSort
        ├── exprs:
        │   ┌── SortOrder { order: Asc }
        │   │   └── #0
        │   └── SortOrder { order: Asc }
        │       └── #1
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
    └── LogicalSort
        ├── exprs:SortOrder { order: Asc }
        │   └── #0
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

