-- TPC-H Q8 without top-most limit node
select
    o_year,
    sum(case
        when nation = 'IRAQ' then volume
        else 0
    end) / sum(volume) as mkt_share
from
    (
        select
            extract(year from o_orderdate) as o_year,
            l_extendedprice * (1 - l_discount) as volume,
            n2.n_name as nation
        from
            part,
            supplier,
            lineitem,
            orders,
            customer,
            nation n1,
            nation n2,
            region
        where
            p_partkey = l_partkey
            and s_suppkey = l_suppkey
            and l_orderkey = o_orderkey
            and o_custkey = c_custkey
            and c_nationkey = n1.n_nationkey
            and n1.n_regionkey = r_regionkey
            and r_name = 'AMERICA'
            and s_nationkey = n2.n_nationkey
            and o_orderdate between date '1995-01-01' and date '1996-12-31'
            and p_type = 'ECONOMY ANODIZED STEEL'
    ) as all_nations
group by
    o_year
order by
    o_year;

/*
LogicalSort
├── exprs:SortOrder { order: Asc }
│   └── #0
└── LogicalProjection
    ├── exprs:
    │   ┌── #0
    │   └── Div
    │       ├── #1
    │       └── #2
    └── LogicalAgg
        ├── exprs:
        │   ┌── Agg(Sum)
        │   │   └── Case
        │   │       └── 
        │   │           ┌── Eq
        │   │           │   ├── #2
        │   │           │   └── "IRAQ"
        │   │           ├── #1
        │   │           └── Cast { cast_to: Decimal128(38, 4), child: 0(i64) }
        │   └── Agg(Sum)
        │       └── [ #1 ]
        ├── groups: [ #0 ]
        └── LogicalProjection
            ├── exprs:
            │   ┌── Scalar(DatePart)
            │   │   └── [ "YEAR", #36 ]
            │   ├── Mul
            │   │   ├── #21
            │   │   └── Sub
            │   │       ├── Cast { cast_to: Decimal128(20, 0), child: 1(i64) }
            │   │       └── #22
            │   └── #54
            └── LogicalFilter
                ├── cond:And
                │   ├── Eq
                │   │   ├── #0
                │   │   └── #17
                │   ├── Eq
                │   │   ├── #9
                │   │   └── #18
                │   ├── Eq
                │   │   ├── #16
                │   │   └── #32
                │   ├── Eq
                │   │   ├── #33
                │   │   └── #41
                │   ├── Eq
                │   │   ├── #44
                │   │   └── #49
                │   ├── Eq
                │   │   ├── #51
                │   │   └── #57
                │   ├── Eq
                │   │   ├── #58
                │   │   └── "AMERICA"
                │   ├── Eq
                │   │   ├── #12
                │   │   └── #53
                │   ├── Between { child: #36, lower: Cast { cast_to: Date32, child: "1995-01-01" }, upper: Cast { cast_to: Date32, child: "1996-12-31" } }
                │   └── Eq
                │       ├── #4
                │       └── "ECONOMY ANODIZED STEEL"
                └── LogicalJoin { join_type: Inner, cond: true }
                    ├── LogicalJoin { join_type: Inner, cond: true }
                    │   ├── LogicalJoin { join_type: Inner, cond: true }
                    │   │   ├── LogicalJoin { join_type: Inner, cond: true }
                    │   │   │   ├── LogicalJoin { join_type: Inner, cond: true }
                    │   │   │   │   ├── LogicalJoin { join_type: Inner, cond: true }
                    │   │   │   │   │   ├── LogicalJoin { join_type: Inner, cond: true }
                    │   │   │   │   │   │   ├── LogicalScan { table: part }
                    │   │   │   │   │   │   └── LogicalScan { table: supplier }
                    │   │   │   │   │   └── LogicalScan { table: lineitem }
                    │   │   │   │   └── LogicalScan { table: orders }
                    │   │   │   └── LogicalScan { table: customer }
                    │   │   └── LogicalScan { table: nation }
                    │   └── LogicalScan { table: nation }
                    └── LogicalScan { table: region }
PhysicalSort
├── exprs:SortOrder { order: Asc }
│   └── #0
└── PhysicalProjection
    ├── exprs:
    │   ┌── #0
    │   └── Div
    │       ├── #1
    │       └── #2
    └── PhysicalGather
        └── PhysicalHashAgg
            ├── aggrs:
            │   ┌── Agg(Sum)
            │   │   └── Case
            │   │       └── 
            │   │           ┌── Eq
            │   │           │   ├── #2
            │   │           │   └── "IRAQ"
            │   │           ├── #1
            │   │           └── Cast { cast_to: Decimal128(38, 4), child: 0(i64) }
            │   └── Agg(Sum)
            │       └── [ #1 ]
            ├── groups: [ #0 ]
            └── PhysicalHashShuffle { columns: [ #0 ] }
                └── PhysicalProjection
                    ├── exprs:
                    │   ┌── Scalar(DatePart)
                    │   │   └── [ "YEAR", #36 ]
                    │   ├── Mul
                    │   │   ├── #21
                    │   │   └── Sub
                    │   │       ├── Cast { cast_to: Decimal128(20, 0), child: 1(i64) }
                    │   │       └── #22
                    │   └── #54
                    └── PhysicalNestedLoopJoin
                        ├── join_type: Inner
                        ├── cond:Eq
                        │   ├── #51
                        │   └── #57
                        ├── PhysicalGather
                        │   └── PhysicalHashJoin { join_type: Inner, left_keys: [ #12 ], right_keys: [ #0 ] }
                        │       ├── PhysicalHashShuffle { columns: [ #12 ] }
                        │       │   └── PhysicalHashJoin { join_type: Inner, left_keys: [ #44 ], right_keys: [ #0 ] }
                        │       │       ├── PhysicalHashShuffle { columns: [ #44 ] }
                        │       │       │   └── PhysicalHashJoin { join_type: Inner, left_keys: [ #33 ], right_keys: [ #0 ] }
                        │       │       │       ├── PhysicalHashShuffle { columns: [ #33 ] }
                        │       │       │       │   └── PhysicalHashJoin { join_type: Inner, left_keys: [ #16 ], right_keys: [ #0 ] }
                        │       │       │       │       ├── PhysicalHashShuffle { columns: [ #16 ] }
                        │       │       │       │       │   └── PhysicalHashJoin { join_type: Inner, left_keys: [ #0, #9 ], right_keys: [ #1, #2 ] }
                        │       │       │       │       │       ├── PhysicalHashShuffle { columns: [ #0, #9 ] }
                        │       │       │       │       │       │   └── PhysicalNestedLoopJoin { join_type: Inner, cond: true }
                        │       │       │       │       │       │       ├── PhysicalFilter
                        │       │       │       │       │       │       │   ├── cond:Eq
                        │       │       │       │       │       │       │   │   ├── #4
                        │       │       │       │       │       │       │   │   └── "ECONOMY ANODIZED STEEL"
                        │       │       │       │       │       │       │   └── PhysicalGather
                        │       │       │       │       │       │       │       └── PhysicalScan { table: part }
                        │       │       │       │       │       │       └── PhysicalGather
                        │       │       │       │       │       │           └── PhysicalScan { table: supplier }
                        │       │       │       │       │       └── PhysicalHashShuffle { columns: [ #1, #2 ] }
                        │       │       │       │       │           └── PhysicalScan { table: lineitem }
                        │       │       │       │       └── PhysicalFilter { cond: Between { child: #4, lower: Cast { cast_to: Date32, child: "1995-01-01" }, upper: Cast { cast_to: Date32, child: "1996-12-31" } } }
                        │       │       │       │           └── PhysicalHashShuffle { columns: [ #0 ] }
                        │       │       │       │               └── PhysicalScan { table: orders }
                        │       │       │       └── PhysicalHashShuffle { columns: [ #0 ] }
                        │       │       │           └── PhysicalScan { table: customer }
                        │       │       └── PhysicalHashShuffle { columns: [ #0 ] }
                        │       │           └── PhysicalScan { table: nation }
                        │       └── PhysicalHashShuffle { columns: [ #0 ] }
                        │           └── PhysicalScan { table: nation }
                        └── PhysicalFilter
                            ├── cond:Eq
                            │   ├── #1
                            │   └── "AMERICA"
                            └── PhysicalGather
                                └── PhysicalScan { table: region }
*/

