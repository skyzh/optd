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
PhysicalStreamAgg
├── aggrs:Agg(Sum)
│   └── [ Cast { cast_to: Int64, child: #1 } ]
├── groups: [ #0 ]
└── PhysicalSort
    ├── exprs:SortOrder { order: Asc }
    │   └── #0
    └── PhysicalScan { table: t1 }
0 200
2 202
3 201
group_id=!2
  subgoal_id=.19 winner=24 weighted_cost=1000 | (PhysicalScan P0)
    cost={compute=0,io=1000}
    stat={row_cnt=1000}
    (required) sort=<any>
    (derived) sort=<any>
  subgoal_id=.28 winner=(Enforcer)30 weighted_cost=8008.754779315221 | (PhysicalSort !2 P10) goal=.19
    cost={compute=7008.754779315221,io=1000}
    stat={row_cnt=1000}
    (required) sort=[AnySorted#0]
    (derived) sort=[Asc#0]
  subgoal_id=.36 winner=(Enforcer)30 weighted_cost=8008.754779315221 | (PhysicalSort !2 P10) goal=.19
    cost={compute=7008.754779315221,io=1000}
    stat={row_cnt=1000}
    (required) sort=[Asc#0]
    (derived) sort=[Asc#0]
  schema=[t1v1:Int32, t1v2:Int32]
  column_ref=[t1.0, t1.1]
  expr_id=1 | (Scan P0)
  expr_id=24 | (PhysicalScan P0)
  P0=(Constant(Utf8String) "t1")
group_id=!6
  subgoal_id=.14 winner=22 weighted_cost=9180 | (PhysicalHashAgg !2 P3 P4)
    cost={compute=8180,io=1000}
    stat={row_cnt=1000}
    (required) sort=<any>
    (derived) sort=<any>
  subgoal_id=.15 winner=22 weighted_cost=9180 | (PhysicalHashAgg !2 P3 P4)
    cost={compute=8180,io=1000}
    stat={row_cnt=1000}
    (required) sort=<any>
    (derived) sort=<any>
  subgoal_id=.34 winner=27 weighted_cost=16108.75477931522 | (PhysicalStreamAgg !2 P3 P4)
    cost={compute=15108.75477931522,io=1000}
    stat={row_cnt=1000}
    (required) sort=[Asc#0]
    (derived) sort=[Asc#0]
  schema=[unnamed:Binary, unnamed:UInt64]
  column_ref=[t1.0, Derived]
  expr_id=5 | (Agg !2 P3 P4)
  expr_id=8 | (Projection !6 P7)
  expr_id=22 | (PhysicalHashAgg !2 P3 P4)
  expr_id=27 | (PhysicalStreamAgg !2 P3 P4)
  expr_id=33 | (PhysicalProjection !6 P7)
  P3=(List (Func(Agg("sum")) (List (Cast (ColumnRef 1(u64)) (DataType(Int64))))))
  P4=(List (ColumnRef 0(u64)))
  P7=(List (ColumnRef 0(u64)) (ColumnRef 1(u64)))
group_id=!12
  subgoal_id=.13 winner=!9.34 weighted_cost=16108.75477931522 | !9.34
    cost={compute=15108.75477931522,io=1000}
    stat={row_cnt=1000}
    (required) sort=<any>
    (derived) sort=[Asc#0]
  schema=[unnamed:UInt64, unnamed:UInt64]
  column_ref=[t1.0, Derived]
  expr_id=11 | (Sort !6 P10)
  expr_id=18 | (PhysicalSort !6 P10)
  P10=(List (SortOrder(Asc) (ColumnRef 0(u64))))
group_id=!31
  schema=[t1v1:Int32, t1v2:Int32]
  column_ref=[t1.0, t1.1]
  expr_id=30 | (PhysicalSort !2 P10)
  P10=(List (SortOrder(Asc) (ColumnRef 0(u64))))
*/

