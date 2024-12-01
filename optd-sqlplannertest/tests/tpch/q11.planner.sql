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
PhysicalSort
├── exprs:SortOrder { order: Desc }
│   └── #1
└── PhysicalFilter
    ├── cond:Gt
    │   ├── Cast { cast_to: Decimal128(38, 15), child: #1 }
    │   └── #2
    └── PhysicalHashAgg
        ├── aggrs:Agg(Sum)
        │   └── Mul
        │       ├── #5
        │       └── Cast { cast_to: Decimal128(10, 0), child: #4 }
        ├── groups: [ #0, #1 ]
        └── PhysicalNestedLoopJoin { join_type: Inner, cond: true }
            ├── PhysicalHashAgg
            │   ├── aggrs:Agg(Sum)
            │   │   └── Mul
            │   │       ├── #3
            │   │       └── Cast { cast_to: Decimal128(10, 0), child: #2 }
            │   ├── groups: [ #0 ]
            │   └── PhysicalHashJoin { join_type: Inner, left_keys: [ #8 ], right_keys: [ #0 ] }
            │       ├── PhysicalHashJoin { join_type: Inner, left_keys: [ #1 ], right_keys: [ #0 ] }
            │       │   ├── PhysicalScan { table: partsupp }
            │       │   └── PhysicalScan { table: supplier }
            │       └── PhysicalFilter
            │           ├── cond:Eq
            │           │   ├── #1
            │           │   └── "CHINA"
            │           └── PhysicalScan { table: nation }
            └── PhysicalHashJoin { join_type: Inner, left_keys: [ #8 ], right_keys: [ #0 ] }
                ├── PhysicalHashJoin { join_type: Inner, left_keys: [ #1 ], right_keys: [ #0 ] }
                │   ├── PhysicalScan { table: partsupp }
                │   └── PhysicalScan { table: supplier }
                └── PhysicalFilter
                    ├── cond:Eq
                    │   ├── #1
                    │   └── "CHINA"
                    └── PhysicalScan { table: nation }
*/

