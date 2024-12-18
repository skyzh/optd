-- (no id or description)
create table t1(t1v1 int, t1v2 int);
create table t2(t2v1 int, t2v3 int);
create table t3(t3v2 int, t3v4 int);
insert into t1 values (0, 0), (1, 1), (2, 2);
insert into t2 values (0, 200), (1, 201), (2, 202);
insert into t3 values (0, 300), (1, 301), (2, 302);

/*
3
3
3
*/

-- Test whether the optimizer enumerates all 2-join orders.
select * from t2, t1 where t1v1 = t2v1;

/*
(Join t1 t2)
(Join t2 t1)

(Join t1 t2)
(Join t2 t1)

0 200 0 0
1 201 1 1
2 202 2 2
*/

-- Test whether the optimizer enumerates all 3-join orders. (It should)
select * from t2, t1, t3 where t1v1 = t2v1 and t1v1 = t3v2;

/*
(Join t1 (Join t2 t3))
(Join t1 (Join t3 t2))
(Join t2 (Join t1 t3))
(Join t2 (Join t3 t1))
(Join t3 (Join t1 t2))
(Join t3 (Join t2 t1))
(Join (Join t1 t2) t3)
(Join (Join t1 t3) t2)
(Join (Join t2 t1) t3)
(Join (Join t2 t3) t1)
(Join (Join t3 t1) t2)
(Join (Join t3 t2) t1)

(Join t1 (Join t2 t3))
(Join t1 (Join t3 t2))
(Join t2 (Join t1 t3))
(Join t2 (Join t3 t1))
(Join t3 (Join t1 t2))
(Join t3 (Join t2 t1))
(Join (Join t1 t2) t3)
(Join (Join t1 t3) t2)
(Join (Join t2 t1) t3)
(Join (Join t2 t3) t1)
(Join (Join t3 t1) t2)
(Join (Join t3 t2) t1)

0 200 0 0 0 300
1 201 1 1 1 301
2 202 2 2 2 302
*/

-- Test whether the optimizer enumerates all 3-join orders. (It don't currently)
select * from t2, t1, t3 where t1v1 = t2v1 and t1v2 = t3v2;

/*
(Join t1 (Join t2 t3))
(Join t1 (Join t3 t2))
(Join t2 (Join t1 t3))
(Join t2 (Join t3 t1))
(Join t3 (Join t1 t2))
(Join t3 (Join t2 t1))
(Join (Join t1 t2) t3)
(Join (Join t1 t3) t2)
(Join (Join t2 t1) t3)
(Join (Join t2 t3) t1)
(Join (Join t3 t1) t2)
(Join (Join t3 t2) t1)

(Join t1 (Join t2 t3))
(Join t1 (Join t3 t2))
(Join t2 (Join t1 t3))
(Join t2 (Join t3 t1))
(Join t3 (Join t1 t2))
(Join t3 (Join t2 t1))
(Join (Join t1 t2) t3)
(Join (Join t1 t3) t2)
(Join (Join t2 t1) t3)
(Join (Join t2 t3) t1)
(Join (Join t3 t1) t2)
(Join (Join t3 t2) t1)

0 200 0 0 0 300
1 201 1 1 1 301
2 202 2 2 2 302
*/

-- Test whether the optimizer enumerates all 3-join orders. (It don't currently)
select * from t1, t2, t3 where t1v1 = t2v1 and t1v2 = t3v2;

/*
(Join t1 (Join t2 t3))
(Join t1 (Join t3 t2))
(Join t2 (Join t1 t3))
(Join t2 (Join t3 t1))
(Join t3 (Join t1 t2))
(Join t3 (Join t2 t1))
(Join (Join t1 t2) t3)
(Join (Join t1 t3) t2)
(Join (Join t2 t1) t3)
(Join (Join t2 t3) t1)
(Join (Join t3 t1) t2)
(Join (Join t3 t2) t1)

(Join t1 (Join t2 t3))
(Join t1 (Join t3 t2))
(Join t2 (Join t1 t3))
(Join t2 (Join t3 t1))
(Join t3 (Join t1 t2))
(Join t3 (Join t2 t1))
(Join (Join t1 t2) t3)
(Join (Join t1 t3) t2)
(Join (Join t2 t1) t3)
(Join (Join t2 t3) t1)
(Join (Join t3 t1) t2)
(Join (Join t3 t2) t1)

0 0 0 200 0 300
1 1 1 201 1 301
2 2 2 202 2 302
*/

-- Test whether the optimizer enumerates all 3-join orders. (It don't currently)
select * from t1, (select * from t2, t3) where t1v1 = t2v1 and t1v2 = t3v2;

/*
(Join t1 (Join t2 t3))
(Join t1 (Join t3 t2))
(Join t2 (Join t1 t3))
(Join t2 (Join t3 t1))
(Join t3 (Join t1 t2))
(Join t3 (Join t2 t1))
(Join (Join t1 t2) t3)
(Join (Join t1 t3) t2)
(Join (Join t2 t1) t3)
(Join (Join t2 t3) t1)
(Join (Join t3 t1) t2)
(Join (Join t3 t2) t1)

PhysicalGather
└── PhysicalHashJoin { join_type: Inner, left_keys: [ #1 ], right_keys: [ #0 ] }
    ├── PhysicalHashShuffle { columns: [ #1 ] }
    │   └── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ] }
    │       ├── PhysicalHashShuffle { columns: [ #0 ] }
    │       │   └── PhysicalScan { table: t1 }
    │       └── PhysicalHashShuffle { columns: [ #0 ] }
    │           └── PhysicalScan { table: t2 }
    └── PhysicalHashShuffle { columns: [ #0 ] }
        └── PhysicalScan { table: t3 }
(Join t1 (Join t2 t3))
(Join t1 (Join t3 t2))
(Join t2 (Join t1 t3))
(Join t2 (Join t3 t1))
(Join t3 (Join t1 t2))
(Join t3 (Join t2 t1))
(Join (Join t1 t2) t3)
(Join (Join t1 t3) t2)
(Join (Join t2 t1) t3)
(Join (Join t2 t3) t1)
(Join (Join t3 t1) t2)
(Join (Join t3 t2) t1)

PhysicalProjection { exprs: [ #4, #5, #0, #1, #2, #3 ] }
└── PhysicalGather
    └── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #2 ] }
        ├── PhysicalHashShuffle { columns: [ #0 ] }
        │   └── PhysicalScan { table: t2 }
        └── PhysicalHashShuffle { columns: [ #2 ] }
            └── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #1 ] }
                ├── PhysicalHashShuffle { columns: [ #0 ] }
                │   └── PhysicalScan { table: t3 }
                └── PhysicalHashShuffle { columns: [ #1 ] }
                    └── PhysicalScan { table: t1 }
0 0 0 200 0 300
1 1 1 201 1 301
2 2 2 202 2 302
*/

