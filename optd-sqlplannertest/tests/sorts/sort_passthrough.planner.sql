-- (no id or description)
create table t1(t1v1 int, t1v2 int);
insert into t1 values (0, 200), (3, 201), (2, 202);

/*
3
*/

-- test order by passthrough
select * from (select * from (select * from (select * from t1 order by t1v1 limit 1) order by t1v1 limit 1) order by t1v1 limit 1) order by t1v1 limit 1;

/*
LogicalLimit { skip: 0(i64), fetch: 1(i64) }
└── LogicalSort
    ├── exprs:SortOrder { order: Asc }
    │   └── #0
    └── LogicalProjection { exprs: [ #0, #1 ] }
        └── LogicalLimit { skip: 0(i64), fetch: 1(i64) }
            └── LogicalSort
                ├── exprs:SortOrder { order: Asc }
                │   └── #0
                └── LogicalProjection { exprs: [ #0, #1 ] }
                    └── LogicalLimit { skip: 0(i64), fetch: 1(i64) }
                        └── LogicalSort
                            ├── exprs:SortOrder { order: Asc }
                            │   └── #0
                            └── LogicalProjection { exprs: [ #0, #1 ] }
                                └── LogicalLimit { skip: 0(i64), fetch: 1(i64) }
                                    └── LogicalSort
                                        ├── exprs:SortOrder { order: Asc }
                                        │   └── #0
                                        └── LogicalProjection { exprs: [ #0, #1 ] }
                                            └── LogicalScan { table: t1 }
PhysicalLimit { skip: 0(i64), fetch: 1(i64) }
└── PhysicalLimit { skip: 0(i64), fetch: 1(i64) }
    └── PhysicalLimit { skip: 0(i64), fetch: 1(i64) }
        └── PhysicalLimit { skip: 0(i64), fetch: 1(i64) }
            └── PhysicalSort
                ├── exprs:SortOrder { order: Asc }
                │   └── #0
                └── PhysicalGather
                    └── PhysicalScan { table: t1 }
0 200
group_id=!2
  subgoal_id=.39 winner=(Enforcer)44 weighted_cost=8008.754779315221 | (PhysicalSort !2 P3) goal=.42
    cost={compute=7008.754779315221,io=1000}
    stat={row_cnt=1000}
    (required) sort=[Asc#0]
    (required) distribution=Any
    (derived) sort=[Asc#0]
    (derived) distribution=SomeShard
  subgoal_id=.42 winner=41 weighted_cost=1000 | (PhysicalScan P0)
    cost={compute=0,io=1000}
    stat={row_cnt=1000}
    (required) sort=<any>
    (required) distribution=Any
    (derived) sort=<any>
    (derived) distribution=SomeShard
  subgoal_id=.51 winner=(Enforcer)44 weighted_cost=8118.754779315221 | (PhysicalSort !2 P3) goal=.52
    cost={compute=7108.754779315221,io=1010}
    stat={row_cnt=1000}
    (required) sort=[Asc#0]
    (required) distribution=Single
    (derived) sort=[Asc#0]
    (derived) distribution=Single
  subgoal_id=.52 winner=(Enforcer)53 weighted_cost=1110 | (PhysicalGather !2) goal=.42
    cost={compute=100,io=1010}
    stat={row_cnt=1000}
    (required) sort=<any>
    (required) distribution=Single
    (derived) sort=<any>
    (derived) distribution=Single
  schema=[t1v1:Int32, t1v2:Int32]
  column_ref=[t1.0, t1.1]
  expr_id=1 | (Scan P0)
  expr_id=41 | (PhysicalScan P0)
  P0=(Constant(Utf8String) "t1")
group_id=!5
  subgoal_id=.38 winner=!2.39 weighted_cost=8008.754779315221 | !2.39
    cost={compute=7008.754779315221,io=1000}
    stat={row_cnt=1000}
    (required) sort=<any>
    (required) distribution=Any
    (derived) sort=[Asc#0]
    (derived) distribution=SomeShard
  subgoal_id=.49 winner=!2.51 weighted_cost=8118.754779315221 | !2.51
    cost={compute=7108.754779315221,io=1010}
    stat={row_cnt=1000}
    (required) sort=[Asc#0]
    (required) distribution=Single
    (derived) sort=[Asc#0]
    (derived) distribution=Single
  subgoal_id=.85 winner=44 weighted_cost=8118.754779315221 | (PhysicalSort !2 P3)
    cost={compute=7108.754779315221,io=1010}
    stat={row_cnt=1000}
    (required) sort=<any>
    (required) distribution=Single
    (derived) sort=[Asc#0]
    (derived) distribution=Single
  schema=[t1v1:Int32, t1v2:Int32]
  column_ref=[t1.0, t1.1]
  expr_id=4 | (Sort !2 P3)
  expr_id=44 | (PhysicalSort !2 P3)
  P3=(List (SortOrder(Asc) (ColumnRef 0(u64))))
group_id=!9
  subgoal_id=.37 winner=48 weighted_cost=9218.75477931522 | (PhysicalLimit !5 P6 P7)
    cost={compute=8208.75477931522,io=1010}
    stat={row_cnt=1}
    (required) sort=[Asc#0]
    (required) distribution=Any
    (derived) sort=[Asc#0]
    (derived) distribution=Single
  subgoal_id=.61 winner=<unknown>
    (required) sort=<any>
    (required) distribution=Any
  subgoal_id=.64 winner=48 weighted_cost=9218.75477931522 | (PhysicalLimit !5 P6 P7)
    cost={compute=8208.75477931522,io=1010}
    stat={row_cnt=1}
    (required) sort=[Asc#0]
    (required) distribution=Single
    (derived) sort=[Asc#0]
    (derived) distribution=Single
  subgoal_id=.84 winner=48 weighted_cost=9218.75477931522 | (PhysicalLimit !5 P6 P7)
    cost={compute=8208.75477931522,io=1010}
    stat={row_cnt=1}
    (required) sort=<any>
    (required) distribution=Single
    (derived) sort=[Asc#0]
    (derived) distribution=Single
  schema=[t1v1:Int32, t1v2:Int32]
  column_ref=[t1.0, t1.1]
  expr_id=8 | (Limit !5 P6 P7)
  expr_id=48 | (PhysicalLimit !5 P6 P7)
  P6=(Constant(Int64) 0(i64))
  P7=(Constant(Int64) 1(i64))
group_id=!12
  subgoal_id=.36 winner=!9.37 weighted_cost=9218.75477931522 | !9.37
    cost={compute=8208.75477931522,io=1010}
    stat={row_cnt=1}
    (required) sort=<any>
    (required) distribution=Any
    (derived) sort=[Asc#0]
    (derived) distribution=Single
  subgoal_id=.60 winner=!9.64 weighted_cost=9218.75477931522 | !9.64
    cost={compute=8208.75477931522,io=1010}
    stat={row_cnt=1}
    (required) sort=[Asc#0]
    (required) distribution=Single
    (derived) sort=[Asc#0]
    (derived) distribution=Single
  subgoal_id=.83 winner=!9.64 weighted_cost=9218.75477931522 | !9.64
    cost={compute=8208.75477931522,io=1010}
    stat={row_cnt=1}
    (required) sort=<any>
    (required) distribution=Single
    (derived) sort=[Asc#0]
    (derived) distribution=Single
  schema=[t1v1:Int32, t1v2:Int32]
  column_ref=[t1.0, t1.1]
  expr_id=11 | (Sort !9 P3)
  expr_id=63 | (PhysicalSort !9 P3)
  P3=(List (SortOrder(Asc) (ColumnRef 0(u64))))
group_id=!16
  subgoal_id=.35 winner=59 weighted_cost=9219.854779315221 | (PhysicalLimit !12 P6 P7)
    cost={compute=8209.854779315221,io=1010}
    stat={row_cnt=1}
    (required) sort=[Asc#0]
    (required) distribution=Any
    (derived) sort=[Asc#0]
    (derived) distribution=Single
  subgoal_id=.69 winner=<unknown>
    (required) sort=<any>
    (required) distribution=Any
  subgoal_id=.72 winner=59 weighted_cost=9219.854779315221 | (PhysicalLimit !12 P6 P7)
    cost={compute=8209.854779315221,io=1010}
    stat={row_cnt=1}
    (required) sort=[Asc#0]
    (required) distribution=Single
    (derived) sort=[Asc#0]
    (derived) distribution=Single
  subgoal_id=.82 winner=59 weighted_cost=9219.854779315221 | (PhysicalLimit !12 P6 P7)
    cost={compute=8209.854779315221,io=1010}
    stat={row_cnt=1}
    (required) sort=<any>
    (required) distribution=Single
    (derived) sort=[Asc#0]
    (derived) distribution=Single
  schema=[t1v1:Int32, t1v2:Int32]
  column_ref=[t1.0, t1.1]
  expr_id=15 | (Limit !12 P6 P7)
  expr_id=59 | (PhysicalLimit !12 P6 P7)
  P6=(Constant(Int64) 0(i64))
  P7=(Constant(Int64) 1(i64))
group_id=!19
  subgoal_id=.34 winner=!16.35 weighted_cost=9219.854779315221 | !16.35
    cost={compute=8209.854779315221,io=1010}
    stat={row_cnt=1}
    (required) sort=<any>
    (required) distribution=Any
    (derived) sort=[Asc#0]
    (derived) distribution=Single
  subgoal_id=.68 winner=!16.72 weighted_cost=9219.854779315221 | !16.72
    cost={compute=8209.854779315221,io=1010}
    stat={row_cnt=1}
    (required) sort=[Asc#0]
    (required) distribution=Single
    (derived) sort=[Asc#0]
    (derived) distribution=Single
  subgoal_id=.81 winner=!16.72 weighted_cost=9219.854779315221 | !16.72
    cost={compute=8209.854779315221,io=1010}
    stat={row_cnt=1}
    (required) sort=<any>
    (required) distribution=Single
    (derived) sort=[Asc#0]
    (derived) distribution=Single
  schema=[t1v1:Int32, t1v2:Int32]
  column_ref=[t1.0, t1.1]
  expr_id=18 | (Sort !16 P3)
  expr_id=71 | (PhysicalSort !16 P3)
  P3=(List (SortOrder(Asc) (ColumnRef 0(u64))))
group_id=!23
  subgoal_id=.33 winner=67 weighted_cost=9220.954779315221 | (PhysicalLimit !19 P6 P7)
    cost={compute=8210.954779315221,io=1010}
    stat={row_cnt=1}
    (required) sort=[Asc#0]
    (required) distribution=Any
    (derived) sort=[Asc#0]
    (derived) distribution=Single
  subgoal_id=.77 winner=<unknown>
    (required) sort=<any>
    (required) distribution=Any
  subgoal_id=.80 winner=67 weighted_cost=9220.954779315221 | (PhysicalLimit !19 P6 P7)
    cost={compute=8210.954779315221,io=1010}
    stat={row_cnt=1}
    (required) sort=<any>
    (required) distribution=Single
    (derived) sort=[Asc#0]
    (derived) distribution=Single
  subgoal_id=.86 winner=67 weighted_cost=9220.954779315221 | (PhysicalLimit !19 P6 P7)
    cost={compute=8210.954779315221,io=1010}
    stat={row_cnt=1}
    (required) sort=[Asc#0]
    (required) distribution=Single
    (derived) sort=[Asc#0]
    (derived) distribution=Single
  schema=[t1v1:Int32, t1v2:Int32]
  column_ref=[t1.0, t1.1]
  expr_id=22 | (Limit !19 P6 P7)
  expr_id=67 | (PhysicalLimit !19 P6 P7)
  P6=(Constant(Int64) 0(i64))
  P7=(Constant(Int64) 1(i64))
group_id=!26
  subgoal_id=.32 winner=!23.33 weighted_cost=9220.954779315221 | !23.33
    cost={compute=8210.954779315221,io=1010}
    stat={row_cnt=1}
    (required) sort=<any>
    (required) distribution=Any
    (derived) sort=[Asc#0]
    (derived) distribution=Single
  subgoal_id=.76 winner=!23.86 weighted_cost=9220.954779315221 | !23.86
    cost={compute=8210.954779315221,io=1010}
    stat={row_cnt=1}
    (required) sort=<any>
    (required) distribution=Single
    (derived) sort=[Asc#0]
    (derived) distribution=Single
  schema=[t1v1:Int32, t1v2:Int32]
  column_ref=[t1.0, t1.1]
  expr_id=25 | (Sort !23 P3)
  expr_id=79 | (PhysicalSort !23 P3)
  P3=(List (SortOrder(Asc) (ColumnRef 0(u64))))
group_id=!30
  subgoal_id=.31 winner=75 weighted_cost=9222.054779315222 | (PhysicalLimit !26 P6 P7)
    cost={compute=8212.054779315222,io=1010}
    stat={row_cnt=1}
    (required) sort=<any>
    (required) distribution=Single
    (derived) sort=[Asc#0]
    (derived) distribution=Single
  schema=[t1v1:Int32, t1v2:Int32]
  column_ref=[t1.0, t1.1]
  expr_id=29 | (Limit !26 P6 P7)
  expr_id=75 | (PhysicalLimit !26 P6 P7)
  P6=(Constant(Int64) 0(i64))
  P7=(Constant(Int64) 1(i64))
group_id=!54
  schema=[t1v1:Int32, t1v2:Int32]
  column_ref=[t1.0, t1.1]
  expr_id=53 | (PhysicalGather !2)
*/

-- test order by passthrough
select * from (select * from t1 order by t1v1 limit 1) order by t1v1 limit 1;

/*
LogicalLimit { skip: 0(i64), fetch: 1(i64) }
└── LogicalSort
    ├── exprs:SortOrder { order: Asc }
    │   └── #0
    └── LogicalProjection { exprs: [ #0, #1 ] }
        └── LogicalLimit { skip: 0(i64), fetch: 1(i64) }
            └── LogicalSort
                ├── exprs:SortOrder { order: Asc }
                │   └── #0
                └── LogicalProjection { exprs: [ #0, #1 ] }
                    └── LogicalScan { table: t1 }
PhysicalLimit { skip: 0(i64), fetch: 1(i64) }
└── PhysicalLimit { skip: 0(i64), fetch: 1(i64) }
    └── PhysicalSort
        ├── exprs:SortOrder { order: Asc }
        │   └── #0
        └── PhysicalGather
            └── PhysicalScan { table: t1 }
0 200
*/

-- test order by passthrough
select * from (select * from t1 order by t1v1, t1v2 limit 1) order by t1v1 limit 1;

/*
LogicalLimit { skip: 0(i64), fetch: 1(i64) }
└── LogicalSort
    ├── exprs:SortOrder { order: Asc }
    │   └── #0
    └── LogicalProjection { exprs: [ #0, #1 ] }
        └── LogicalLimit { skip: 0(i64), fetch: 1(i64) }
            └── LogicalSort
                ├── exprs:
                │   ┌── SortOrder { order: Asc }
                │   │   └── #0
                │   └── SortOrder { order: Asc }
                │       └── #1
                └── LogicalProjection { exprs: [ #0, #1 ] }
                    └── LogicalScan { table: t1 }
PhysicalLimit { skip: 0(i64), fetch: 1(i64) }
└── PhysicalLimit { skip: 0(i64), fetch: 1(i64) }
    └── PhysicalSort
        ├── exprs:
        │   ┌── SortOrder { order: Asc }
        │   │   └── #0
        │   └── SortOrder { order: Asc }
        │       └── #1
        └── PhysicalGather
            └── PhysicalScan { table: t1 }
0 200
*/

-- test order by passthrough
select * from (select * from t1 order by t1v1 limit 1) order by t1v1, t1v2 limit 1;

/*
LogicalLimit { skip: 0(i64), fetch: 1(i64) }
└── LogicalSort
    ├── exprs:
    │   ┌── SortOrder { order: Asc }
    │   │   └── #0
    │   └── SortOrder { order: Asc }
    │       └── #1
    └── LogicalProjection { exprs: [ #0, #1 ] }
        └── LogicalLimit { skip: 0(i64), fetch: 1(i64) }
            └── LogicalSort
                ├── exprs:SortOrder { order: Asc }
                │   └── #0
                └── LogicalProjection { exprs: [ #0, #1 ] }
                    └── LogicalScan { table: t1 }
PhysicalLimit { skip: 0(i64), fetch: 1(i64) }
└── PhysicalLimit { skip: 0(i64), fetch: 1(i64) }
    └── PhysicalSort
        ├── exprs:
        │   ┌── SortOrder { order: Asc }
        │   │   └── #0
        │   └── SortOrder { order: Asc }
        │       └── #1
        └── PhysicalGather
            └── PhysicalScan { table: t1 }
0 200
*/

