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
select * from t1 as a, t1 as b where a.t1v1 = b.t1v1;

/*
(Join t1 t1)

LogicalProjection { exprs: [ #0, #1, #2, #3 ] }
└── LogicalFilter
    ├── cond:Eq
    │   ├── #0
    │   └── #2
    └── LogicalJoin { join_type: Cross, cond: true }
        ├── LogicalScan { table: t1 }
        └── LogicalScan { table: t1 }
PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ] }
├── PhysicalScan { table: t1 }
└── PhysicalScan { table: t1 }
0 0 0 0
1 1 1 1
2 2 2 2
group_id=!2 subgroup_id=.73 winner=75 weighted_cost=1000 | (PhysicalScan P0)
  cost={compute=0,io=1000}
  stat={row_cnt=1000}
  sort=<any>
  schema=[t1v1:Int32, t1v2:Int32]
  column_ref=[t1.0, t1.1]
  expr_id=1 | (Scan P0)
  expr_id=75 | (PhysicalScan P0)
  P0=(Constant(Utf8String) "t1")
group_id=!6 subgroup_id=.68 winner=72 weighted_cost=1003000 | (PhysicalNestedLoopJoin(Cross) !2 !2 P4)
  cost={compute=1001000,io=2000}
  stat={row_cnt=10000}
  sort=<any>
  schema=[t1v1:Int32, t1v2:Int32, t1v1:Int32, t1v2:Int32]
  column_ref=[t1.0, t1.1, t1.0, t1.1]
  expr_id=5 | (Join(Cross) !2 !2 P4)
  expr_id=24 | (Projection !6 P21)
  expr_id=70 | (PhysicalProjection !6 P21)
  expr_id=72 | (PhysicalNestedLoopJoin(Cross) !2 !2 P4)
  P4=(Constant(Bool) true)
  P21=(List (ColumnRef 0(u64)) (ColumnRef 1(u64)) (ColumnRef 2(u64)) (ColumnRef 3(u64)))
group_id=!15 subgroup_id=.10 winner=84 weighted_cost=5000 | (PhysicalHashJoin(Inner) !2 !2 P82 P82)
  cost={compute=3000,io=2000}
  stat={row_cnt=1000}
  sort=<any>
  schema=[t1v1:Int32, t1v2:Int32, t1v1:Int32, t1v2:Int32]
  column_ref=[t1.0, t1.1, t1.0, t1.1]
  expr_id=8 | (Filter !6 P7)
  expr_id=12 | (Join(Inner) !2 !2 P7)
  expr_id=14 | (Join(Inner) !2 !2 P13)
  expr_id=17 | (Projection !15 P16)
  expr_id=44 | (Projection !15 P21)
  expr_id=50 | (Filter !48 P13)
  expr_id=59 | (PhysicalProjection !15 P16)
  expr_id=62 | (PhysicalFilter !48 P13)
  expr_id=77 | (PhysicalProjection !15 P21)
  expr_id=84 | (PhysicalHashJoin(Inner) !2 !2 P82 P82)
  expr_id=86 | (PhysicalNestedLoopJoin(Inner) !2 !2 P7)
  expr_id=88 | (PhysicalFilter !6 P7)
  expr_id=92 | (PhysicalNestedLoopJoin(Inner) !2 !2 P13)
  P7=(BinOp(Eq) (ColumnRef 0(u64)) (ColumnRef 2(u64)))
  P13=(BinOp(Eq) (ColumnRef 2(u64)) (ColumnRef 0(u64)))
  P16=(List (ColumnRef 2(u64)) (ColumnRef 3(u64)) (ColumnRef 0(u64)) (ColumnRef 1(u64)))
  P21=(List (ColumnRef 0(u64)) (ColumnRef 1(u64)) (ColumnRef 2(u64)) (ColumnRef 3(u64)))
  P82=(List (ColumnRef 0(u64)))
group_id=!15 subgroup_id=.60 winner=84 weighted_cost=5000 | (PhysicalHashJoin(Inner) !2 !2 P82 P82)
  cost={compute=3000,io=2000}
  stat={row_cnt=1000}
  sort=<any>
  schema=[t1v1:Int32, t1v2:Int32, t1v1:Int32, t1v2:Int32]
  column_ref=[t1.0, t1.1, t1.0, t1.1]
  expr_id=8 | (Filter !6 P7)
  expr_id=12 | (Join(Inner) !2 !2 P7)
  expr_id=14 | (Join(Inner) !2 !2 P13)
  expr_id=17 | (Projection !15 P16)
  expr_id=44 | (Projection !15 P21)
  expr_id=50 | (Filter !48 P13)
  expr_id=59 | (PhysicalProjection !15 P16)
  expr_id=62 | (PhysicalFilter !48 P13)
  expr_id=77 | (PhysicalProjection !15 P21)
  expr_id=84 | (PhysicalHashJoin(Inner) !2 !2 P82 P82)
  expr_id=86 | (PhysicalNestedLoopJoin(Inner) !2 !2 P7)
  expr_id=88 | (PhysicalFilter !6 P7)
  expr_id=92 | (PhysicalNestedLoopJoin(Inner) !2 !2 P13)
  P7=(BinOp(Eq) (ColumnRef 0(u64)) (ColumnRef 2(u64)))
  P13=(BinOp(Eq) (ColumnRef 2(u64)) (ColumnRef 0(u64)))
  P16=(List (ColumnRef 2(u64)) (ColumnRef 3(u64)) (ColumnRef 0(u64)) (ColumnRef 1(u64)))
  P21=(List (ColumnRef 0(u64)) (ColumnRef 1(u64)) (ColumnRef 2(u64)) (ColumnRef 3(u64)))
  P82=(List (ColumnRef 0(u64)))
group_id=!48 subgroup_id=.63 winner=67 weighted_cost=1053000 | (PhysicalProjection !6 P16)
  cost={compute=1051000,io=2000}
  stat={row_cnt=10000}
  sort=<any>
  schema=[unnamed:UInt64, unnamed:UInt64, unnamed:UInt64, unnamed:UInt64]
  column_ref=[t1.0, t1.1, t1.0, t1.1]
  expr_id=36 | (Projection !6 P16)
  expr_id=47 | (Projection !48 P21)
  expr_id=65 | (PhysicalProjection !48 P21)
  expr_id=67 | (PhysicalProjection !6 P16)
  P16=(List (ColumnRef 2(u64)) (ColumnRef 3(u64)) (ColumnRef 0(u64)) (ColumnRef 1(u64)))
  P21=(List (ColumnRef 0(u64)) (ColumnRef 1(u64)) (ColumnRef 2(u64)) (ColumnRef 3(u64)))
*/

