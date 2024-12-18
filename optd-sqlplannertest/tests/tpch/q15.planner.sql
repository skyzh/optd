-- TPC-H Q15
WITH revenue0 (supplier_no, total_revenue) AS 
(
    SELECT
        l_suppkey,
        SUM(l_extendedprice * (1 - l_discount)) 
    FROM
        lineitem 
    WHERE
        l_shipdate >= DATE '1993-01-01' 
        AND l_shipdate < DATE '1993-01-01' + INTERVAL '3' MONTH 
    GROUP BY
        l_suppkey 
)
SELECT
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue 
FROM
    supplier,
    revenue0 
WHERE
    s_suppkey = supplier_no 
    AND total_revenue = 
    (
        SELECT
            MAX(total_revenue) 
        FROM
            revenue0 
    )
ORDER BY
    s_suppkey;

/*
LogicalSort
├── exprs:SortOrder { order: Asc }
│   └── #0
└── LogicalProjection { exprs: [ #0, #1, #2, #4, #8 ] }
    └── LogicalFilter
        ├── cond:And
        │   ├── Eq
        │   │   ├── #0
        │   │   └── #7
        │   └── Eq
        │       ├── #8
        │       └── #9
        └── DependentJoin { join_type: Inner, cond: true, extern_cols: [] }
            ├── LogicalJoin { join_type: Inner, cond: true }
            │   ├── LogicalScan { table: supplier }
            │   └── LogicalProjection { exprs: [ #0, #1 ] }
            │       └── LogicalProjection { exprs: [ #0, #1 ] }
            │           └── LogicalAgg
            │               ├── exprs:Agg(Sum)
            │               │   └── Mul
            │               │       ├── #5
            │               │       └── Sub
            │               │           ├── Cast { cast_to: Decimal128(20, 0), child: 1(i64) }
            │               │           └── #6
            │               ├── groups: [ #2 ]
            │               └── LogicalFilter
            │                   ├── cond:And
            │                   │   ├── Geq
            │                   │   │   ├── #10
            │                   │   │   └── Cast { cast_to: Date32, child: "1993-01-01" }
            │                   │   └── Lt
            │                   │       ├── #10
            │                   │       └── Add
            │                   │           ├── Cast { cast_to: Date32, child: "1993-01-01" }
            │                   │           └── INTERVAL_MONTH_DAY_NANO (3, 0, 0)
            │                   └── LogicalScan { table: lineitem }
            └── LogicalProjection { exprs: [ #0 ] }
                └── LogicalAgg
                    ├── exprs:Agg(Max)
                    │   └── [ #1 ]
                    ├── groups: []
                    └── LogicalProjection { exprs: [ #0, #1 ] }
                        └── LogicalProjection { exprs: [ #0, #1 ] }
                            └── LogicalAgg
                                ├── exprs:Agg(Sum)
                                │   └── Mul
                                │       ├── #5
                                │       └── Sub
                                │           ├── Cast { cast_to: Decimal128(20, 0), child: 1(i64) }
                                │           └── #6
                                ├── groups: [ #2 ]
                                └── LogicalFilter
                                    ├── cond:And
                                    │   ├── Geq
                                    │   │   ├── #10
                                    │   │   └── Cast { cast_to: Date32, child: "1993-01-01" }
                                    │   └── Lt
                                    │       ├── #10
                                    │       └── Add
                                    │           ├── Cast { cast_to: Date32, child: "1993-01-01" }
                                    │           └── INTERVAL_MONTH_DAY_NANO (3, 0, 0)
                                    └── LogicalScan { table: lineitem }
PhysicalSort
├── exprs:SortOrder { order: Asc }
│   └── #0
└── PhysicalProjection { exprs: [ #0, #1, #2, #4, #8 ] }
    └── PhysicalFilter
        ├── cond:And
        │   └── Eq
        │       ├── #8
        │       └── #9
        └── PhysicalGather
            └── PhysicalHashAgg
                ├── aggrs:Agg(Max)
                │   └── [ #10 ]
                ├── groups: [ #0, #1, #2, #3, #4, #5, #6, #7, #8 ]
                └── PhysicalHashShuffle { columns: [ #0, #1, #2, #3, #4, #5, #6, #7, #8 ] }
                    └── PhysicalHashAgg
                        ├── aggrs:Agg(Sum)
                        │   └── Mul
                        │       ├── #14
                        │       └── Sub
                        │           ├── Cast { cast_to: Decimal128(20, 0), child: 1(i64) }
                        │           └── #15
                        ├── groups: [ #0, #1, #2, #3, #4, #5, #6, #7, #8, #11 ]
                        └── PhysicalHashShuffle { columns: [ #0, #1, #2, #3, #4, #5, #6, #7, #8, #11 ] }
                            └── PhysicalProjection { exprs: [ #16, #17, #18, #19, #20, #21, #22, #23, #24, #0, #1, #2, #3, #4, #5, #6, #7, #8, #9, #10, #11, #12, #13, #14, #15 ] }
                                └── PhysicalProjection { exprs: [ #2, #3, #4, #5, #6, #7, #8, #9, #10, #11, #12, #13, #14, #15, #16, #17, #18, #19, #20, #21, #22, #23, #24, #0, #1 ] }
                                    └── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ] }
                                        ├── PhysicalHashShuffle { columns: [ #0 ] }
                                        │   └── PhysicalFilter
                                        │       ├── cond:And
                                        │       │   ├── Geq
                                        │       │   │   ├── #12
                                        │       │   │   └── Cast { cast_to: Date32, child: "1993-01-01" }
                                        │       │   └── Lt
                                        │       │       ├── #12
                                        │       │       └── Add
                                        │       │           ├── Cast { cast_to: Date32, child: "1993-01-01" }
                                        │       │           └── INTERVAL_MONTH_DAY_NANO (3, 0, 0)
                                        │       └── PhysicalNestedLoopJoin { join_type: Inner, cond: true }
                                        │           ├── PhysicalGather
                                        │           │   └── PhysicalHashAgg
                                        │           │       ├── aggrs:Agg(Sum)
                                        │           │       │   └── Mul
                                        │           │       │       ├── #5
                                        │           │       │       └── Sub
                                        │           │       │           ├── Cast { cast_to: Decimal128(20, 0), child: 1(i64) }
                                        │           │       │           └── #6
                                        │           │       ├── groups: [ #2 ]
                                        │           │       └── PhysicalFilter
                                        │           │           ├── cond:And
                                        │           │           │   ├── Geq
                                        │           │           │   │   ├── #10
                                        │           │           │   │   └── Cast { cast_to: Date32, child: "1993-01-01" }
                                        │           │           │   └── Lt
                                        │           │           │       ├── #10
                                        │           │           │       └── Add
                                        │           │           │           ├── Cast { cast_to: Date32, child: "1993-01-01" }
                                        │           │           │           └── INTERVAL_MONTH_DAY_NANO (3, 0, 0)
                                        │           │           └── PhysicalHashShuffle { columns: [ #2 ] }
                                        │           │               └── PhysicalScan { table: lineitem }
                                        │           └── PhysicalGather
                                        │               └── PhysicalScan { table: lineitem }
                                        └── PhysicalHashShuffle { columns: [ #0 ] }
                                            └── PhysicalScan { table: supplier }
*/

