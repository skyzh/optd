-- TPC-H Q11
select
    ps_partkey,
    sum(ps_supplycost * ps_availqty) as value
from
    partsupp,
    supplier,
    nation
where
    ps_suppkey = s_suppkey
    and s_nationkey = n_nationkey
    and n_name = 'CHINA'
group by
    ps_partkey having
        sum(ps_supplycost * ps_availqty) > (
            select
                sum(ps_supplycost * ps_availqty) * 0.0001000000
            from
                partsupp,
                supplier,
                nation
            where
                ps_suppkey = s_suppkey
                and s_nationkey = n_nationkey
                and n_name = 'CHINA'
        )
order by
    value desc;

/*
LogicalSort
├── exprs:SortOrder { order: Desc }
│   └── #1
└── LogicalProjection { exprs: [ #0, #1 ] }
    └── LogicalFilter
        ├── cond:Gt
        │   ├── Cast { cast_to: Decimal128(38, 15), child: #1 }
        │   └── #2
        └── DependentJoin { join_type: Inner, cond: true, extern_cols: [] }
            ├── LogicalAgg
            │   ├── exprs:Agg(Sum)
            │   │   └── Mul
            │   │       ├── #3
            │   │       └── Cast { cast_to: Decimal128(10, 0), child: #2 }
            │   ├── groups: [ #0 ]
            │   └── LogicalFilter
            │       ├── cond:And
            │       │   ├── Eq
            │       │   │   ├── #1
            │       │   │   └── #5
            │       │   ├── Eq
            │       │   │   ├── #8
            │       │   │   └── #12
            │       │   └── Eq
            │       │       ├── #13
            │       │       └── "CHINA"
            │       └── LogicalJoin { join_type: Inner, cond: true }
            │           ├── LogicalJoin { join_type: Inner, cond: true }
            │           │   ├── LogicalScan { table: partsupp }
            │           │   └── LogicalScan { table: supplier }
            │           └── LogicalScan { table: nation }
            └── LogicalProjection
                ├── exprs:Cast
                │   ├── cast_to: Decimal128(38, 15)
                │   ├── child:Mul
                │   │   ├── Cast { cast_to: Float64, child: #0 }
                │   │   └── 0.0001(float)

                └── LogicalAgg
                    ├── exprs:Agg(Sum)
                    │   └── Mul
                    │       ├── #3
                    │       └── Cast { cast_to: Decimal128(10, 0), child: #2 }
                    ├── groups: []
                    └── LogicalFilter
                        ├── cond:And
                        │   ├── Eq
                        │   │   ├── #1
                        │   │   └── #5
                        │   ├── Eq
                        │   │   ├── #8
                        │   │   └── #12
                        │   └── Eq
                        │       ├── #13
                        │       └── "CHINA"
                        └── LogicalJoin { join_type: Inner, cond: true }
                            ├── LogicalJoin { join_type: Inner, cond: true }
                            │   ├── LogicalScan { table: partsupp }
                            │   └── LogicalScan { table: supplier }
                            └── LogicalScan { table: nation }
PhysicalFilter
├── cond:Gt
│   ├── Cast { cast_to: Decimal128(38, 15), child: #1 }
│   └── #2
└── PhysicalSort
    ├── exprs:SortOrder { order: Desc }
    │   └── #1
    └── PhysicalGather
        └── PhysicalHashAgg
            ├── aggrs:Agg(Sum)
            │   └── Mul
            │       ├── #5
            │       └── Cast { cast_to: Decimal128(10, 0), child: #4 }
            ├── groups: [ #0, #1 ]
            └── PhysicalHashShuffle { columns: [ #0, #1 ] }
                └── PhysicalProjection { exprs: [ #11, #12, #13, #14, #15, #16, #17, #4, #5, #6, #7, #8, #9, #10, #0, #1, #2, #3 ] }
                    └── PhysicalHashJoin { join_type: Inner, left_keys: [ #4 ], right_keys: [ #1 ] }
                        ├── PhysicalHashShuffle { columns: [ #4 ] }
                        │   └── PhysicalNestedLoopJoin { join_type: Inner, cond: true }
                        │       ├── PhysicalGather
                        │       │   └── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #3 ] }
                        │       │       ├── PhysicalFilter
                        │       │       │   ├── cond:Eq
                        │       │       │   │   ├── #1
                        │       │       │   │   └── "CHINA"
                        │       │       │   └── PhysicalHashShuffle { columns: [ #0 ] }
                        │       │       │       └── PhysicalScan { table: nation }
                        │       │       └── PhysicalHashShuffle { columns: [ #3 ] }
                        │       │           └── PhysicalScan { table: supplier }
                        │       └── PhysicalGather
                        │           └── PhysicalHashAgg
                        │               ├── aggrs:Agg(Sum)
                        │               │   └── Mul
                        │               │       ├── #3
                        │               │       └── Cast { cast_to: Decimal128(10, 0), child: #2 }
                        │               ├── groups: [ #0 ]
                        │               └── PhysicalHashShuffle { columns: [ #0 ] }
                        │                   └── PhysicalProjection { exprs: [ #4, #5, #6, #7, #8, #9, #10, #11, #12, #13, #14, #15, #0, #1, #2, #3 ] }
                        │                       └── PhysicalProjection { exprs: [ #0, #1, #2, #3, #11, #12, #13, #14, #15, #4, #5, #6, #7, #8, #9, #10 ] }
                        │                           └── PhysicalHashJoin { join_type: Inner, left_keys: [ #4 ], right_keys: [ #1 ] }
                        │                               ├── PhysicalHashShuffle { columns: [ #4 ] }
                        │                               │   └── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #3 ] }
                        │                               │       ├── PhysicalFilter
                        │                               │       │   ├── cond:Eq
                        │                               │       │   │   ├── #1
                        │                               │       │   │   └── "CHINA"
                        │                               │       │   └── PhysicalHashShuffle { columns: [ #0 ] }
                        │                               │       │       └── PhysicalScan { table: nation }
                        │                               │       └── PhysicalHashShuffle { columns: [ #3 ] }
                        │                               │           └── PhysicalScan { table: supplier }
                        │                               └── PhysicalHashShuffle { columns: [ #1 ] }
                        │                                   └── PhysicalScan { table: partsupp }
                        └── PhysicalHashShuffle { columns: [ #1 ] }
                            └── PhysicalScan { table: partsupp }
*/

