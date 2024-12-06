-- TPC-H Q17
SELECT
    ROUND(SUM(l_extendedprice) / 7.0, 16) AS avg_yearly 
FROM
    lineitem,
    part 
WHERE
    p_partkey = l_partkey 
    AND p_brand = 'Brand#13' 
    AND p_container = 'JUMBO PKG' 
    AND l_quantity < ( 
        SELECT
            0.2 * AVG(l_quantity) 
        FROM
            lineitem 
        WHERE
            l_partkey = p_partkey 
    );

/*
LogicalProjection
├── exprs:Scalar(Round)
│   └── 
│       ┌── Div
│       │   ├── Cast { cast_to: Float64, child: #0 }
│       │   └── 7(float)
│       └── 16(i64)
└── LogicalAgg
    ├── exprs:Agg(Sum)
    │   └── [ #5 ]
    ├── groups: []
    └── LogicalFilter
        ├── cond:And
        │   ├── Eq
        │   │   ├── #16
        │   │   └── #1
        │   ├── Eq
        │   │   ├── #19
        │   │   └── "Brand#13"
        │   ├── Eq
        │   │   ├── #22
        │   │   └── "JUMBO PKG"
        │   └── Lt
        │       ├── Cast { cast_to: Decimal128(30, 15), child: #4 }
        │       └── #25
        └── DependentJoin { join_type: Inner, cond: true, extern_cols: [ Extern(#16) ] }
            ├── LogicalJoin { join_type: Inner, cond: true }
            │   ├── LogicalScan { table: lineitem }
            │   └── LogicalScan { table: part }
            └── LogicalProjection
                ├── exprs:Cast
                │   ├── cast_to: Decimal128(30, 15)
                │   ├── child:Mul
                │   │   ├── 0.2(float)
                │   │   └── Cast { cast_to: Float64, child: #0 }

                └── LogicalAgg
                    ├── exprs:Agg(Avg)
                    │   └── [ #4 ]
                    ├── groups: []
                    └── LogicalFilter
                        ├── cond:Eq
                        │   ├── #1
                        │   └── Extern(#16)
                        └── LogicalScan { table: lineitem }
PhysicalProjection
├── exprs:Scalar(Round)
│   └── 
│       ┌── Div
│       │   ├── Cast { cast_to: Float64, child: #0 }
│       │   └── 7(float)
│       └── 16(i64)
└── PhysicalGather
    └── PhysicalStreamAgg
        ├── aggrs:Agg(Sum)
        │   └── [ #5 ]
        ├── groups: []
        └── PhysicalHashShuffle { columns: [] }
            └── PhysicalFilter
                ├── cond:And
                │   └── Lt
                │       ├── Cast { cast_to: Decimal128(30, 15), child: #4 }
                │       └── #25
                └── PhysicalHashAgg
                    ├── aggrs:Agg(Avg)
                    │   └── [ #29 ]
                    ├── groups: [ #0, #1, #2, #3, #4, #5, #6, #7, #8, #9, #10, #11, #12, #13, #14, #15, #16, #17, #18, #19, #20, #21, #22, #23, #24 ]
                    └── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #1 ] }
                        ├── PhysicalHashShuffle { columns: [ #0 ] }
                        │   └── PhysicalHashJoin { join_type: Inner, left_keys: [ #1 ], right_keys: [ #0 ] }
                        │       ├── PhysicalHashShuffle { columns: [ #1 ] }
                        │       │   └── PhysicalScan { table: lineitem }
                        │       └── PhysicalFilter
                        │           ├── cond:And
                        │           │   ├── Eq
                        │           │   │   ├── #3
                        │           │   │   └── "Brand#13"
                        │           │   └── Eq
                        │           │       ├── #6
                        │           │       └── "JUMBO PKG"
                        │           └── PhysicalHashShuffle { columns: [ #0 ] }
                        │               └── PhysicalScan { table: part }
                        └── PhysicalHashShuffle { columns: [ #1 ] }
                            └── PhysicalScan { table: lineitem }
*/

