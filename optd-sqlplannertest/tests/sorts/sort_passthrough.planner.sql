-- (no id or description)
create table t1(t1v1 int, t1v2 int);
insert into t2 values (0, 200), (3, 201), (2, 202);

/*
Error
Error during planning: table 'datafusion.public.t2' not found
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
group_id=!2
  subgroup_id=.27 winner=29 weighted_cost=1000 | (PhysicalScan P0)
    cost={compute=0,io=1000}
    stat={row_cnt=1000}
    sort=<any>
  subgroup_id=.30 winner=enforcer weighted_cost=7908.754779315221 | enforcer
    cost={compute=6908.754779315221,io=1000}
    stat={row_cnt=1000}
    sort=[Asc#0]
  schema=[t1v1:Int32, t1v2:Int32]
  column_ref=[t1.0, t1.1]
  expr_id=1 | (Scan P0)
  expr_id=29 | (PhysicalScan P0)
  P0=(Constant(Utf8String) "t1")
group_id=!5
  subgroup_id=.24 winner=26 weighted_cost=7908.754779315221 | (PhysicalSort !2 P3)
    cost={compute=6908.754779315221,io=1000}
    stat={row_cnt=1000}
    sort=<any>
  subgroup_id=.31 winner=26 weighted_cost=7908.754779315221 | (PhysicalSort !2 P3)
    cost={compute=6908.754779315221,io=1000}
    stat={row_cnt=1000}
    sort=[Asc#0]
  schema=[t1v1:Int32, t1v2:Int32]
  column_ref=[t1.0, t1.1]
  expr_id=4 | (Sort !2 P3)
  expr_id=26 | (PhysicalSort !2 P3)
  P3=(List (SortOrder(Asc) (ColumnRef 0(u64))))
group_id=!8
  subgroup_id=.21 winner=!5 weighted_cost=7908.754779315221 | !5
    cost={compute=6908.754779315221,io=1000}
    stat={row_cnt=1000}
    sort=<any>
  subgroup_id=.32 winner=!5 weighted_cost=7908.754779315221 | !5
    cost={compute=6908.754779315221,io=1000}
    stat={row_cnt=1000}
    sort=[Asc#0]
  schema=[t1v1:Int32, t1v2:Int32]
  column_ref=[t1.0, t1.1]
  expr_id=7 | (Sort !5 P3)
  expr_id=23 | (PhysicalSort !5 P3)
  P3=(List (SortOrder(Asc) (ColumnRef 0(u64))))
group_id=!11
  subgroup_id=.18 winner=!8 weighted_cost=7908.754779315221 | !8
    cost={compute=6908.754779315221,io=1000}
    stat={row_cnt=1000}
    sort=<any>
  subgroup_id=.33 winner=!8 weighted_cost=7908.754779315221 | !8
    cost={compute=6908.754779315221,io=1000}
    stat={row_cnt=1000}
    sort=[Asc#0]
  schema=[t1v1:Int32, t1v2:Int32]
  column_ref=[t1.0, t1.1]
  expr_id=10 | (Sort !8 P3)
  expr_id=20 | (PhysicalSort !8 P3)
  P3=(List (SortOrder(Asc) (ColumnRef 0(u64))))
group_id=!14
  subgroup_id=.15 winner=!11 weighted_cost=7908.754779315221 | !11
    cost={compute=6908.754779315221,io=1000}
    stat={row_cnt=1000}
    sort=<any>
  schema=[t1v1:Int32, t1v2:Int32]
  column_ref=[t1.0, t1.1]
  expr_id=13 | (Sort !11 P3)
  expr_id=17 | (PhysicalSort !11 P3)
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
└── PhysicalSort
    ├── exprs:SortOrder { order: Asc }
    │   └── #0
    └── PhysicalScan { table: t1 }
*/

