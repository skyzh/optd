-- TPC-H Q9
SELECT
    nation,
    o_year,
    SUM(amount) AS sum_profit
FROM
    (
        SELECT
            n_name AS nation,
            EXTRACT(YEAR FROM o_orderdate) AS o_year,
            l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
        FROM
            part,
            supplier,
            lineitem,
            partsupp,
            orders,
            nation
        WHERE
            s_suppkey = l_suppkey
            AND ps_suppkey = l_suppkey
            AND ps_partkey = l_partkey
            AND p_partkey = l_partkey
            AND o_orderkey = l_orderkey
            AND s_nationkey = n_nationkey
            AND p_name LIKE '%green%'
    ) AS profit
GROUP BY
    nation,
    o_year
ORDER BY
    nation,
    o_year DESC;

/*
LogicalSort
├── exprs:
│   ┌── SortOrder { order: Asc }
│   │   └── #0
│   └── SortOrder { order: Desc }
│       └── #1
└── LogicalProjection { exprs: [ #0, #1, #2 ] }
    └── LogicalAgg
        ├── exprs:Agg(Sum)
        │   └── [ #2 ]
        ├── groups: [ #0, #1 ]
        └── LogicalProjection
            ├── exprs:
            │   ┌── #47
            │   ├── Scalar(DatePart)
            │   │   └── [ "YEAR", #41 ]
            │   └── Sub
            │       ├── Mul
            │       │   ├── #21
            │       │   └── Sub
            │       │       ├── Cast { cast_to: Decimal128(20, 0), child: 1(i64) }
            │       │       └── #22
            │       └── Mul
            │           ├── #35
            │           └── #20
            └── LogicalFilter
                ├── cond:And
                │   ├── Eq
                │   │   ├── #9
                │   │   └── #18
                │   ├── Eq
                │   │   ├── #33
                │   │   └── #18
                │   ├── Eq
                │   │   ├── #32
                │   │   └── #17
                │   ├── Eq
                │   │   ├── #0
                │   │   └── #17
                │   ├── Eq
                │   │   ├── #37
                │   │   └── #16
                │   ├── Eq
                │   │   ├── #12
                │   │   └── #46
                │   └── Like { expr: #1, pattern: "%green%", negated: false, case_insensitive: false }
                └── LogicalJoin { join_type: Inner, cond: true }
                    ├── LogicalJoin { join_type: Inner, cond: true }
                    │   ├── LogicalJoin { join_type: Inner, cond: true }
                    │   │   ├── LogicalJoin { join_type: Inner, cond: true }
                    │   │   │   ├── LogicalJoin { join_type: Inner, cond: true }
                    │   │   │   │   ├── LogicalScan { table: part }
                    │   │   │   │   └── LogicalScan { table: supplier }
                    │   │   │   └── LogicalScan { table: lineitem }
                    │   │   └── LogicalScan { table: partsupp }
                    │   └── LogicalScan { table: orders }
                    └── LogicalScan { table: nation }
PhysicalGather
└── PhysicalStreamAgg
    ├── aggrs:Agg(Sum)
    │   └── [ #2 ]
    ├── groups: [ #0, #1 ]
    └── PhysicalSort
        ├── exprs:
        │   ┌── SortOrder { order: Asc }
        │   │   └── #0
        │   └── SortOrder { order: Desc }
        │       └── #1
        └── PhysicalHashShuffle { columns: [ #0, #1 ] }
            └── PhysicalProjection
                ├── exprs:
                │   ┌── #47
                │   ├── Scalar(DatePart)
                │   │   └── [ "YEAR", #41 ]
                │   └── Sub
                │       ├── Mul
                │       │   ├── #21
                │       │   └── Sub
                │       │       ├── Cast { cast_to: Decimal128(20, 0), child: 1(i64) }
                │       │       └── #22
                │       └── Mul
                │           ├── #35
                │           └── #20
                └── PhysicalHashJoin { join_type: Inner, left_keys: [ #12 ], right_keys: [ #0 ] }
                    ├── PhysicalHashShuffle { columns: [ #12 ] }
                    │   └── PhysicalHashJoin { join_type: Inner, left_keys: [ #16 ], right_keys: [ #0 ] }
                    │       ├── PhysicalHashShuffle { columns: [ #16 ] }
                    │       │   └── PhysicalHashJoin { join_type: Inner, left_keys: [ #18, #17 ], right_keys: [ #1, #0 ] }
                    │       │       ├── PhysicalHashShuffle { columns: [ #18, #17 ] }
                    │       │       │   └── PhysicalHashJoin { join_type: Inner, left_keys: [ #9, #0 ], right_keys: [ #2, #1 ] }
                    │       │       │       ├── PhysicalHashShuffle { columns: [ #9, #0 ] }
                    │       │       │       │   └── PhysicalNestedLoopJoin { join_type: Inner, cond: true }
                    │       │       │       │       ├── PhysicalFilter { cond: Like { expr: #1, pattern: "%green%", negated: false, case_insensitive: false } }
                    │       │       │       │       │   └── PhysicalGather
                    │       │       │       │       │       └── PhysicalScan { table: part }
                    │       │       │       │       └── PhysicalGather
                    │       │       │       │           └── PhysicalScan { table: supplier }
                    │       │       │       └── PhysicalHashShuffle { columns: [ #2, #1 ] }
                    │       │       │           └── PhysicalScan { table: lineitem }
                    │       │       └── PhysicalHashShuffle { columns: [ #1, #0 ] }
                    │       │           └── PhysicalScan { table: partsupp }
                    │       └── PhysicalHashShuffle { columns: [ #0 ] }
                    │           └── PhysicalScan { table: orders }
                    └── PhysicalHashShuffle { columns: [ #0 ] }
                        └── PhysicalScan { table: nation }
*/

