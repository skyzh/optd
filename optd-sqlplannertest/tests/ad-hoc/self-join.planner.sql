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
select * from (select * from t1 as a, t1 as b where a.t1v1 = b.t1v1 order by a.t1v1) order by a.t1v1;

/*
(Join t1 t1)

LogicalSort
├── exprs:SortOrder { order: Asc }
│   └── #0
└── LogicalProjection { exprs: [ #0, #1, #2, #3 ] }
    └── LogicalSort
        ├── exprs:SortOrder { order: Asc }
        │   └── #0
        └── LogicalProjection { exprs: [ #0, #1, #2, #3 ] }
            └── LogicalFilter
                ├── cond:Eq
                │   ├── #0
                │   └── #2
                └── LogicalJoin { join_type: Cross, cond: true }
                    ├── LogicalScan { table: t1 }
                    └── LogicalScan { table: t1 }
PhysicalSort
├── exprs:SortOrder { order: Asc }
│   └── #0
└── PhysicalSort
    ├── exprs:SortOrder { order: Asc }
    │   └── #0
    └── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ] }
        ├── PhysicalScan { table: t1 }
        └── PhysicalScan { table: t1 }
0 0 0 0
1 1 1 1
2 2 2 2
group_id=!2 subgroup_id=.97 winner=99 weighted_cost=1000 | (PhysicalScan P0)
  cost={compute=0,io=1000}
  stat={row_cnt=1000}
  sort=<any>
  schema=[t1v1:Int32, t1v2:Int32]
  column_ref=[t1.0, t1.1]
  expr_id=1 | (Scan P0)
  expr_id=99 | (PhysicalScan P0)
  P0=(Constant(Utf8String) "t1")
group_id=!6 subgroup_id=.90 winner=96 weighted_cost=1003000 | (PhysicalNestedLoopJoin(Cross) !2 !2 P4)
  cost={compute=1001000,io=2000}
  stat={row_cnt=10000}
  sort=<any>
  schema=[t1v1:Int32, t1v2:Int32, t1v1:Int32, t1v2:Int32]
  column_ref=[t1.0, t1.1, t1.0, t1.1]
  expr_id=5 | (Join(Cross) !2 !2 P4)
  expr_id=52 | (Projection !41 P25)
  expr_id=57 | (Projection !6 P37)
  expr_id=92 | (PhysicalProjection !6 P37)
  expr_id=94 | (PhysicalProjection !41 P25)
  expr_id=96 | (PhysicalNestedLoopJoin(Cross) !2 !2 P4)
  P4=(Constant(Bool) true)
  P25=(List (ColumnRef 2(u64)) (ColumnRef 3(u64)) (ColumnRef 0(u64)) (ColumnRef 1(u64)))
  P37=(List (ColumnRef 0(u64)) (ColumnRef 1(u64)) (ColumnRef 2(u64)) (ColumnRef 3(u64)))
group_id=!6 subgroup_id=.116 winner=96 weighted_cost=1003000 | (PhysicalNestedLoopJoin(Cross) !2 !2 P4)
  cost={compute=1001000,io=2000}
  stat={row_cnt=10000}
  sort=[Asc#0]
  schema=[t1v1:Int32, t1v2:Int32, t1v1:Int32, t1v2:Int32]
  column_ref=[t1.0, t1.1, t1.0, t1.1]
  expr_id=5 | (Join(Cross) !2 !2 P4)
  expr_id=52 | (Projection !41 P25)
  expr_id=57 | (Projection !6 P37)
  expr_id=92 | (PhysicalProjection !6 P37)
  expr_id=94 | (PhysicalProjection !41 P25)
  expr_id=96 | (PhysicalNestedLoopJoin(Cross) !2 !2 P4)
  P4=(Constant(Bool) true)
  P25=(List (ColumnRef 2(u64)) (ColumnRef 3(u64)) (ColumnRef 0(u64)) (ColumnRef 1(u64)))
  P37=(List (ColumnRef 0(u64)) (ColumnRef 1(u64)) (ColumnRef 2(u64)) (ColumnRef 3(u64)))
group_id=!12 subgroup_id=.19 winner=76 weighted_cost=11908.75477931522 | (PhysicalSort !24 P10)
  cost={compute=9908.75477931522,io=2000}
  stat={row_cnt=1000}
  sort=<any>
  schema=[t1v1:Int32, t1v2:Int32, t1v1:Int32, t1v2:Int32]
  column_ref=[t1.0, t1.1, t1.0, t1.1]
  expr_id=11 | (Sort !24 P10)
  expr_id=76 | (PhysicalSort !24 P10)
  P10=(List (SortOrder(Asc) (ColumnRef 0(u64))))
group_id=!12 subgroup_id=.118 winner=76 weighted_cost=11908.75477931522 | (PhysicalSort !24 P10)
  cost={compute=9908.75477931522,io=2000}
  stat={row_cnt=1000}
  sort=[Asc#0]
  schema=[t1v1:Int32, t1v2:Int32, t1v1:Int32, t1v2:Int32]
  column_ref=[t1.0, t1.1, t1.0, t1.1]
  expr_id=11 | (Sort !24 P10)
  expr_id=76 | (PhysicalSort !24 P10)
  P10=(List (SortOrder(Asc) (ColumnRef 0(u64))))
group_id=!15 subgroup_id=.16 winner=18 weighted_cost=18817.50955863044 | (PhysicalSort !12 P10)
  cost={compute=16817.50955863044,io=2000}
  stat={row_cnt=1000}
  sort=<any>
  schema=[t1v1:Int32, t1v2:Int32, t1v1:Int32, t1v2:Int32]
  column_ref=[t1.0, t1.1, t1.0, t1.1]
  expr_id=14 | (Sort !12 P10)
  expr_id=18 | (PhysicalSort !12 P10)
  P10=(List (SortOrder(Asc) (ColumnRef 0(u64))))
group_id=!24 subgroup_id=.77 winner=106 weighted_cost=5000 | (PhysicalHashJoin(Inner) !2 !2 P104 P104)
  cost={compute=3000,io=2000}
  stat={row_cnt=1000}
  sort=<any>
  schema=[t1v1:Int32, t1v2:Int32, t1v1:Int32, t1v2:Int32]
  column_ref=[t1.0, t1.1, t1.0, t1.1]
  expr_id=8 | (Filter !6 P7)
  expr_id=21 | (Join(Inner) !2 !2 P7)
  expr_id=23 | (Join(Inner) !2 !2 P22)
  expr_id=26 | (Projection !24 P25)
  expr_id=38 | (Projection !24 P37)
  expr_id=43 | (Filter !41 P22)
  expr_id=81 | (PhysicalProjection !24 P25)
  expr_id=84 | (PhysicalFilter !41 P22)
  expr_id=101 | (PhysicalProjection !24 P37)
  expr_id=106 | (PhysicalHashJoin(Inner) !2 !2 P104 P104)
  expr_id=108 | (PhysicalNestedLoopJoin(Inner) !2 !2 P7)
  expr_id=110 | (PhysicalFilter !6 P7)
  expr_id=114 | (PhysicalNestedLoopJoin(Inner) !2 !2 P22)
  P7=(BinOp(Eq) (ColumnRef 0(u64)) (ColumnRef 2(u64)))
  P22=(BinOp(Eq) (ColumnRef 2(u64)) (ColumnRef 0(u64)))
  P25=(List (ColumnRef 2(u64)) (ColumnRef 3(u64)) (ColumnRef 0(u64)) (ColumnRef 1(u64)))
  P37=(List (ColumnRef 0(u64)) (ColumnRef 1(u64)) (ColumnRef 2(u64)) (ColumnRef 3(u64)))
  P104=(List (ColumnRef 0(u64)))
group_id=!24 subgroup_id=.82 winner=106 weighted_cost=5000 | (PhysicalHashJoin(Inner) !2 !2 P104 P104)
  cost={compute=3000,io=2000}
  stat={row_cnt=1000}
  sort=<any>
  schema=[t1v1:Int32, t1v2:Int32, t1v1:Int32, t1v2:Int32]
  column_ref=[t1.0, t1.1, t1.0, t1.1]
  expr_id=8 | (Filter !6 P7)
  expr_id=21 | (Join(Inner) !2 !2 P7)
  expr_id=23 | (Join(Inner) !2 !2 P22)
  expr_id=26 | (Projection !24 P25)
  expr_id=38 | (Projection !24 P37)
  expr_id=43 | (Filter !41 P22)
  expr_id=81 | (PhysicalProjection !24 P25)
  expr_id=84 | (PhysicalFilter !41 P22)
  expr_id=101 | (PhysicalProjection !24 P37)
  expr_id=106 | (PhysicalHashJoin(Inner) !2 !2 P104 P104)
  expr_id=108 | (PhysicalNestedLoopJoin(Inner) !2 !2 P7)
  expr_id=110 | (PhysicalFilter !6 P7)
  expr_id=114 | (PhysicalNestedLoopJoin(Inner) !2 !2 P22)
  P7=(BinOp(Eq) (ColumnRef 0(u64)) (ColumnRef 2(u64)))
  P22=(BinOp(Eq) (ColumnRef 2(u64)) (ColumnRef 0(u64)))
  P25=(List (ColumnRef 2(u64)) (ColumnRef 3(u64)) (ColumnRef 0(u64)) (ColumnRef 1(u64)))
  P37=(List (ColumnRef 0(u64)) (ColumnRef 1(u64)) (ColumnRef 2(u64)) (ColumnRef 3(u64)))
  P104=(List (ColumnRef 0(u64)))
group_id=!24 subgroup_id=.115 winner=106 weighted_cost=5000 | (PhysicalHashJoin(Inner) !2 !2 P104 P104)
  cost={compute=3000,io=2000}
  stat={row_cnt=1000}
  sort=[Asc#0]
  schema=[t1v1:Int32, t1v2:Int32, t1v1:Int32, t1v2:Int32]
  column_ref=[t1.0, t1.1, t1.0, t1.1]
  expr_id=8 | (Filter !6 P7)
  expr_id=21 | (Join(Inner) !2 !2 P7)
  expr_id=23 | (Join(Inner) !2 !2 P22)
  expr_id=26 | (Projection !24 P25)
  expr_id=38 | (Projection !24 P37)
  expr_id=43 | (Filter !41 P22)
  expr_id=81 | (PhysicalProjection !24 P25)
  expr_id=84 | (PhysicalFilter !41 P22)
  expr_id=101 | (PhysicalProjection !24 P37)
  expr_id=106 | (PhysicalHashJoin(Inner) !2 !2 P104 P104)
  expr_id=108 | (PhysicalNestedLoopJoin(Inner) !2 !2 P7)
  expr_id=110 | (PhysicalFilter !6 P7)
  expr_id=114 | (PhysicalNestedLoopJoin(Inner) !2 !2 P22)
  P7=(BinOp(Eq) (ColumnRef 0(u64)) (ColumnRef 2(u64)))
  P22=(BinOp(Eq) (ColumnRef 2(u64)) (ColumnRef 0(u64)))
  P25=(List (ColumnRef 2(u64)) (ColumnRef 3(u64)) (ColumnRef 0(u64)) (ColumnRef 1(u64)))
  P37=(List (ColumnRef 0(u64)) (ColumnRef 1(u64)) (ColumnRef 2(u64)) (ColumnRef 3(u64)))
  P104=(List (ColumnRef 0(u64)))
group_id=!41 subgroup_id=.85 winner=89 weighted_cost=1053000 | (PhysicalProjection !6 P25)
  cost={compute=1051000,io=2000}
  stat={row_cnt=10000}
  sort=<any>
  schema=[unnamed:UInt64, unnamed:UInt64, unnamed:UInt64, unnamed:UInt64]
  column_ref=[t1.0, t1.1, t1.0, t1.1]
  expr_id=31 | (Projection !6 P25)
  expr_id=40 | (Projection !41 P37)
  expr_id=87 | (PhysicalProjection !41 P37)
  expr_id=89 | (PhysicalProjection !6 P25)
  P25=(List (ColumnRef 2(u64)) (ColumnRef 3(u64)) (ColumnRef 0(u64)) (ColumnRef 1(u64)))
  P37=(List (ColumnRef 0(u64)) (ColumnRef 1(u64)) (ColumnRef 2(u64)) (ColumnRef 3(u64)))
group_id=!41 subgroup_id=.117 winner=89 weighted_cost=1053000 | (PhysicalProjection !6 P25)
  cost={compute=1051000,io=2000}
  stat={row_cnt=10000}
  sort=[Asc#0]
  schema=[unnamed:UInt64, unnamed:UInt64, unnamed:UInt64, unnamed:UInt64]
  column_ref=[t1.0, t1.1, t1.0, t1.1]
  expr_id=31 | (Projection !6 P25)
  expr_id=40 | (Projection !41 P37)
  expr_id=87 | (PhysicalProjection !41 P37)
  expr_id=89 | (PhysicalProjection !6 P25)
  P25=(List (ColumnRef 2(u64)) (ColumnRef 3(u64)) (ColumnRef 0(u64)) (ColumnRef 1(u64)))
  P37=(List (ColumnRef 0(u64)) (ColumnRef 1(u64)) (ColumnRef 2(u64)) (ColumnRef 3(u64)))
*/

