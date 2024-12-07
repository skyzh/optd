-- (no id or description)
create table t1(t1v1 int, t1v2 int);
insert into t1 values (0, 200), (3, 201), (2, 202);

/*
3
*/

-- test stream agg passthrough
select * from (select t1v1, sum(t1v2) from (select * from t1 order by t1v1) group by t1v1) order by t1v1;

/*
LogicalSort
├── exprs:SortOrder { order: Asc }
│   └── #0
└── LogicalProjection { exprs: [ #0, #1 ] }
    └── LogicalProjection { exprs: [ #0, #1 ] }
        └── LogicalAgg
            ├── exprs:Agg(Sum)
            │   └── [ Cast { cast_to: Int64, child: #1 } ]
            ├── groups: [ #0 ]
            └── LogicalProjection { exprs: [ #0, #1 ] }
                └── LogicalScan { table: t1 }
PhysicalGather
└── PhysicalStreamAgg
    ├── aggrs:Agg(Sum)
    │   └── [ Cast { cast_to: Int64, child: #1 } ]
    ├── groups: [ #0 ]
    └── PhysicalSort
        ├── exprs:SortOrder { order: Asc }
        │   └── #0
        └── PhysicalHashShuffle { columns: [ #0 ] }
            └── PhysicalScan { table: t1 }
0 200
2 202
3 201
group_id=!2
  subgoal_id=.20 winner=26 weighted_cost=1000 | (PhysicalScan P0)
    cost={compute=0,io=1000}
    stat={row_cnt=1000}
    (required) sort=<any>
    (required) distribution=Any
    (derived) sort=<any>
    (derived) distribution=SomeShard
  subgoal_id=.24 winner=(Enforcer)28 weighted_cost=1120 | (PhysicalHashShuffle !2 P4) goal=.20
    cost={compute=110,io=1010}
    stat={row_cnt=1000}
    (required) sort=<any>
    (required) distribution=KeyShard({0})
    (derived) sort=<any>
    (derived) distribution=HashShard([0])
  subgoal_id=.35 winner=(Enforcer)38 weighted_cost=8128.754779315221 | (PhysicalSort !2 P10) goal=.24
    cost={compute=7118.754779315221,io=1010}
    stat={row_cnt=1000}
    (required) sort=[AnySorted#0]
    (required) distribution=KeyShard({0})
    (derived) sort=[Asc#0]
    (derived) distribution=HashShard([0])
  subgoal_id=.40 winner=(Enforcer)38 weighted_cost=8008.754779315221 | (PhysicalSort !2 P10) goal=.20
    cost={compute=7008.754779315221,io=1000}
    stat={row_cnt=1000}
    (required) sort=[AnySorted#0]
    (required) distribution=Any
    (derived) sort=[Asc#0]
    (derived) distribution=SomeShard
  subgoal_id=.50 winner=(Enforcer)38 weighted_cost=8128.754779315221 | (PhysicalSort !2 P10) goal=.24
    cost={compute=7118.754779315221,io=1010}
    stat={row_cnt=1000}
    (required) sort=[Asc#0]
    (required) distribution=KeyShard({0})
    (derived) sort=[Asc#0]
    (derived) distribution=HashShard([0])
  subgoal_id=.53 winner=(Enforcer)38 weighted_cost=8008.754779315221 | (PhysicalSort !2 P10) goal=.20
    cost={compute=7008.754779315221,io=1000}
    stat={row_cnt=1000}
    (required) sort=[Asc#0]
    (required) distribution=Any
    (derived) sort=[Asc#0]
    (derived) distribution=SomeShard
  schema=[t1v1:Int32, t1v2:Int32]
  column_ref=[t1.0, t1.1]
  expr_id=1 | (Scan P0)
  expr_id=26 | (PhysicalScan P0)
  P0=(Constant(Utf8String) "t1")
group_id=!6
  subgoal_id=.14 winner=23 weighted_cost=9300 | (PhysicalHashAgg !2 P3 P4)
    cost={compute=8290,io=1010}
    stat={row_cnt=1000}
    (required) sort=<any>
    (required) distribution=Any
    (derived) sort=<any>
    (derived) distribution=HashShard([0])
  subgoal_id=.15 winner=23 weighted_cost=9300 | (PhysicalHashAgg !2 P3 P4)
    cost={compute=8290,io=1010}
    stat={row_cnt=1000}
    (required) sort=<any>
    (required) distribution=Any
    (derived) sort=<any>
    (derived) distribution=HashShard([0])
  subgoal_id=.19 winner=(Enforcer)30 weighted_cost=9410 | (PhysicalGather !6) goal=.14
    cost={compute=8390,io=1020}
    stat={row_cnt=1000}
    (required) sort=<any>
    (required) distribution=Single
    (derived) sort=<any>
    (derived) distribution=Single
  subgoal_id=.45 winner=(Enforcer)30 weighted_cost=16338.75477931522 | (PhysicalGather !6) goal=.47
    cost={compute=15318.75477931522,io=1020}
    stat={row_cnt=1000}
    (required) sort=[Asc#0]
    (required) distribution=Single
    (derived) sort=[Asc#0]
    (derived) distribution=Single
  subgoal_id=.47 winner=34 weighted_cost=16228.75477931522 | (PhysicalStreamAgg !2 P3 P4)
    cost={compute=15218.75477931522,io=1010}
    stat={row_cnt=1000}
    (required) sort=[Asc#0]
    (required) distribution=Any
    (derived) sort=[Asc#0]
    (derived) distribution=HashShard([0])
  schema=[unnamed:Binary, unnamed:UInt64]
  column_ref=[t1.0, Derived]
  expr_id=5 | (Agg !2 P3 P4)
  expr_id=8 | (Projection !6 P7)
  expr_id=23 | (PhysicalHashAgg !2 P3 P4)
  expr_id=34 | (PhysicalStreamAgg !2 P3 P4)
  expr_id=44 | (PhysicalProjection !6 P7)
  P3=(List (Func(Agg("sum")) (List (Cast (ColumnRef 1(u64)) (DataType(Int64))))))
  P4=(List (ColumnRef 0(u64)))
  P7=(List (ColumnRef 0(u64)) (ColumnRef 1(u64)))
group_id=!12
  subgoal_id=.13 winner=!9.45 weighted_cost=16338.75477931522 | !9.45
    cost={compute=15318.75477931522,io=1020}
    stat={row_cnt=1000}
    (required) sort=<any>
    (required) distribution=Single
    (derived) sort=[Asc#0]
    (derived) distribution=Single
  schema=[unnamed:UInt64, unnamed:UInt64]
  column_ref=[t1.0, Derived]
  expr_id=11 | (Sort !6 P10)
  expr_id=18 | (PhysicalSort !6 P10)
  P10=(List (SortOrder(Asc) (ColumnRef 0(u64))))
group_id=!29
  schema=[t1v1:Int32, t1v2:Int32]
  column_ref=[t1.0, t1.1]
  expr_id=28 | (PhysicalHashShuffle !2 P4)
  P4=(List (ColumnRef 0(u64)))
group_id=!31
  schema=[unnamed:Binary, unnamed:UInt64]
  column_ref=[t1.0, Derived]
  expr_id=30 | (PhysicalGather !6)
group_id=!39
  schema=[t1v1:Int32, t1v2:Int32]
  column_ref=[t1.0, t1.1]
  expr_id=38 | (PhysicalSort !2 P10)
  P10=(List (SortOrder(Asc) (ColumnRef 0(u64))))
*/

