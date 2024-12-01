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
            └── LogicalSort
                ├── exprs:SortOrder { order: Asc }
                │   └── #0
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
  subgoal_id=.23 winner=(Enforcer)28 weighted_cost=8008.754779315221 | (PhysicalSort !2 P3) goal=.26
    cost={compute=7008.754779315221,io=1000}
    stat={row_cnt=1000}
    (required) sort=[Asc#0]
    (derived) sort=[Asc#0]
  subgoal_id=.26 winner=25 weighted_cost=1000 | (PhysicalScan P0)
    cost={compute=0,io=1000}
    stat={row_cnt=1000}
    (required) sort=<any>
    (derived) sort=<any>
  schema=[t1v1:Int32, t1v2:Int32]
  column_ref=[t1.0, t1.1]
  expr_id=1 | (Scan P0)
  expr_id=25 | (PhysicalScan P0)
  P0=(Constant(Utf8String) "t1")
group_id=!5
  subgoal_id=.22 winner=!2.23 weighted_cost=8008.754779315221 | !2.23
    cost={compute=7008.754779315221,io=1000}
    stat={row_cnt=1000}
    (required) sort=<any>
    (derived) sort=[Asc#0]
  subgoal_id=.36 winner=28 weighted_cost=8008.754779315221 | (PhysicalSort !2 P3)
    cost={compute=7008.754779315221,io=1000}
    stat={row_cnt=1000}
    (required) sort=[AnySorted#0]
    (derived) sort=[Asc#0]
  subgoal_id=.42 winner=!2.23 weighted_cost=8008.754779315221 | !2.23
    cost={compute=7008.754779315221,io=1000}
    stat={row_cnt=1000}
    (required) sort=[Asc#0]
    (derived) sort=[Asc#0]
  schema=[t1v1:Int32, t1v2:Int32]
  column_ref=[t1.0, t1.1]
  expr_id=4 | (Sort !2 P3)
  expr_id=28 | (PhysicalSort !2 P3)
  P3=(List (SortOrder(Asc) (ColumnRef 0(u64))))
group_id=!9
  subgoal_id=.17 winner=35 weighted_cost=16108.75477931522 | (PhysicalStreamAgg !5 P6 P7)
    cost={compute=15108.75477931522,io=1000}
    stat={row_cnt=1000}
    (required) sort=<any>
    (derived) sort=[Asc#0]
  subgoal_id=.18 winner=35 weighted_cost=16108.75477931522 | (PhysicalStreamAgg !5 P6 P7)
    cost={compute=15108.75477931522,io=1000}
    stat={row_cnt=1000}
    (required) sort=<any>
    (derived) sort=[Asc#0]
  subgoal_id=.40 winner=35 weighted_cost=16108.75477931522 | (PhysicalStreamAgg !5 P6 P7)
    cost={compute=15108.75477931522,io=1000}
    stat={row_cnt=1000}
    (required) sort=[Asc#0]
    (derived) sort=[Asc#0]
  schema=[unnamed:Binary, unnamed:UInt64]
  column_ref=[t1.0, Derived]
  expr_id=8 | (Agg !5 P6 P7)
  expr_id=11 | (Projection !9 P10)
  expr_id=32 | (PhysicalHashAgg !5 P6 P7)
  expr_id=35 | (PhysicalStreamAgg !5 P6 P7)
  expr_id=39 | (PhysicalProjection !9 P10)
  P6=(List (Func(Agg(Sum)) (List (Cast (ColumnRef 1(u64)) (DataType(Int64))))))
  P7=(List (ColumnRef 0(u64)))
  P10=(List (ColumnRef 0(u64)) (ColumnRef 1(u64)))
group_id=!15
  subgoal_id=.16 winner=!12.40 weighted_cost=16108.75477931522 | !12.40
    cost={compute=15108.75477931522,io=1000}
    stat={row_cnt=1000}
    (required) sort=<any>
    (derived) sort=[Asc#0]
  schema=[unnamed:UInt64, unnamed:UInt64]
  column_ref=[t1.0, Derived]
  expr_id=14 | (Sort !9 P3)
  expr_id=21 | (PhysicalSort !9 P3)
  P3=(List (SortOrder(Asc) (ColumnRef 0(u64))))
*/

