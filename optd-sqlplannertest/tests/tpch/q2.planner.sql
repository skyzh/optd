-- TPC-H Q2
select
        s_acctbal,
        s_name,
        n_name,
        p_partkey,
        p_mfgr,
        s_address,
        s_phone,
        s_comment
from
        part,
        supplier,
        partsupp,
        nation,
        region
where
        p_partkey = ps_partkey
        and s_suppkey = ps_suppkey
and p_size = 4
and p_type like '%TIN'
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'AFRICA'
        and ps_supplycost = (
                select
                        min(ps_supplycost)
                from
                        partsupp,
                        supplier,
                        nation,
                        region
                where
                        p_partkey = ps_partkey
                        and s_suppkey = ps_suppkey
                        and s_nationkey = n_nationkey
                        and n_regionkey = r_regionkey
                        and r_name = 'AFRICA'
        )
order by
    s_acctbal desc,
    n_name,
    s_name,
    p_partkey
limit 100;

/*
LogicalLimit { skip: 0(i64), fetch: 100(i64) }
└── LogicalSort
    ├── exprs:
    │   ┌── SortOrder { order: Desc }
    │   │   └── #0
    │   ├── SortOrder { order: Asc }
    │   │   └── #2
    │   ├── SortOrder { order: Asc }
    │   │   └── #1
    │   └── SortOrder { order: Asc }
    │       └── #3
    └── LogicalProjection { exprs: [ #14, #10, #22, #0, #2, #11, #13, #15 ] }
        └── LogicalFilter
            ├── cond:And
            │   ├── Eq
            │   │   ├── #0
            │   │   └── #16
            │   ├── Eq
            │   │   ├── #9
            │   │   └── #17
            │   ├── Eq
            │   │   ├── Cast { cast_to: Int64, child: #5 }
            │   │   └── 4(i64)
            │   ├── Like { expr: #4, pattern: "%TIN", negated: false, case_insensitive: false }
            │   ├── Eq
            │   │   ├── #12
            │   │   └── #21
            │   ├── Eq
            │   │   ├── #23
            │   │   └── #25
            │   ├── Eq
            │   │   ├── #26
            │   │   └── "AFRICA"
            │   └── Eq
            │       ├── #19
            │       └── #28
            └── DependentJoin { join_type: Inner, cond: true, extern_cols: [ Extern(#0) ] }
                ├── LogicalJoin { join_type: Inner, cond: true }
                │   ├── LogicalJoin { join_type: Inner, cond: true }
                │   │   ├── LogicalJoin { join_type: Inner, cond: true }
                │   │   │   ├── LogicalJoin { join_type: Inner, cond: true }
                │   │   │   │   ├── LogicalScan { table: part }
                │   │   │   │   └── LogicalScan { table: supplier }
                │   │   │   └── LogicalScan { table: partsupp }
                │   │   └── LogicalScan { table: nation }
                │   └── LogicalScan { table: region }
                └── LogicalProjection { exprs: [ #0 ] }
                    └── LogicalAgg
                        ├── exprs:Agg(Min)
                        │   └── [ #3 ]
                        ├── groups: []
                        └── LogicalFilter
                            ├── cond:And
                            │   ├── Eq
                            │   │   ├── Extern(#0)
                            │   │   └── #0
                            │   ├── Eq
                            │   │   ├── #5
                            │   │   └── #1
                            │   ├── Eq
                            │   │   ├── #8
                            │   │   └── #12
                            │   ├── Eq
                            │   │   ├── #14
                            │   │   └── #16
                            │   └── Eq
                            │       ├── #17
                            │       └── "AFRICA"
                            └── LogicalJoin { join_type: Inner, cond: true }
                                ├── LogicalJoin { join_type: Inner, cond: true }
                                │   ├── LogicalJoin { join_type: Inner, cond: true }
                                │   │   ├── LogicalScan { table: partsupp }
                                │   │   └── LogicalScan { table: supplier }
                                │   └── LogicalScan { table: nation }
                                └── LogicalScan { table: region }
LogicalLimit { skip: 0(i64), fetch: 100(i64) }
└── LogicalSort
    ├── exprs:
    │   ┌── SortOrder { order: Desc }
    │   │   └── #0
    │   ├── SortOrder { order: Asc }
    │   │   └── #2
    │   ├── SortOrder { order: Asc }
    │   │   └── #1
    │   └── SortOrder { order: Asc }
    │       └── #3
    └── LogicalProjection { exprs: [ #14, #10, #22, #0, #2, #11, #13, #15 ] }
        └── LogicalFilter
            ├── cond:And
            │   ├── Eq
            │   │   ├── #0
            │   │   └── #16
            │   ├── Eq
            │   │   ├── #9
            │   │   └── #17
            │   ├── Eq
            │   │   ├── Cast { cast_to: Int64, child: #5 }
            │   │   └── 4(i64)
            │   ├── Like { expr: #4, pattern: "%TIN", negated: false, case_insensitive: false }
            │   ├── Eq
            │   │   ├── #12
            │   │   └── #21
            │   ├── Eq
            │   │   ├── #23
            │   │   └── #25
            │   ├── Eq
            │   │   ├── #26
            │   │   └── "AFRICA"
            │   └── Eq
            │       ├── #19
            │       └── #28
            └── LogicalProjection { exprs: [ #0, #1, #2, #3, #4, #5, #6, #7, #8, #9, #10, #11, #12, #13, #14, #15, #16, #17, #18, #19, #20, #21, #22, #23, #24, #25, #26, #27, #28 ] }
                └── LogicalAgg
                    ├── exprs:Agg(Min)
                    │   └── [ #31 ]
                    ├── groups: [ #0, #1, #2, #3, #4, #5, #6, #7, #8, #9, #10, #11, #12, #13, #14, #15, #16, #17, #18, #19, #20, #21, #22, #23, #24, #25, #26, #27 ]
                    └── LogicalFilter
                        ├── cond:And
                        │   ├── Eq
                        │   │   ├── #0
                        │   │   └── #28
                        │   ├── Eq
                        │   │   ├── #33
                        │   │   └── #29
                        │   ├── Eq
                        │   │   ├── #36
                        │   │   └── #40
                        │   ├── Eq
                        │   │   ├── #42
                        │   │   └── #44
                        │   └── Eq
                        │       ├── #45
                        │       └── "AFRICA"
                        └── LogicalJoin { join_type: Inner, cond: true }
                            ├── LogicalJoin { join_type: Inner, cond: true }
                            │   ├── LogicalJoin { join_type: Inner, cond: true }
                            │   │   ├── LogicalJoin { join_type: Inner, cond: true }
                            │   │   │   ├── LogicalJoin { join_type: Inner, cond: true }
                            │   │   │   │   ├── LogicalScan { table: part }
                            │   │   │   │   └── LogicalScan { table: supplier }
                            │   │   │   └── LogicalScan { table: partsupp }
                            │   │   └── LogicalScan { table: nation }
                            │   └── LogicalScan { table: region }
                            └── LogicalJoin { join_type: Inner, cond: true }
                                ├── LogicalJoin { join_type: Inner, cond: true }
                                │   ├── LogicalJoin { join_type: Inner, cond: true }
                                │   │   ├── LogicalScan { table: partsupp }
                                │   │   └── LogicalScan { table: supplier }
                                │   └── LogicalScan { table: nation }
                                └── LogicalScan { table: region }
PhysicalLimit { skip: 0(i64), fetch: 100(i64) }
└── PhysicalGather
    └── PhysicalSort
        ├── exprs:
        │   ┌── SortOrder { order: Desc }
        │   │   └── #0
        │   ├── SortOrder { order: Asc }
        │   │   └── #2
        │   ├── SortOrder { order: Asc }
        │   │   └── #1
        │   └── SortOrder { order: Asc }
        │       └── #3
        └── PhysicalProjection { exprs: [ #14, #10, #22, #0, #2, #11, #13, #15 ] }
            └── PhysicalFilter
                ├── cond:And
                │   └── Eq
                │       ├── #19
                │       └── #28
                └── PhysicalHashAgg
                    ├── aggrs:Agg(Min)
                    │   └── [ #31 ]
                    ├── groups: [ #0, #1, #2, #3, #4, #5, #6, #7, #8, #9, #10, #11, #12, #13, #14, #15, #16, #17, #18, #19, #20, #21, #22, #23, #24, #25, #26, #27 ]
                    └── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ] }
                        ├── PhysicalHashShuffle { columns: [ #0 ] }
                        │   └── PhysicalHashJoin { join_type: Inner, left_keys: [ #23 ], right_keys: [ #0 ] }
                        │       ├── PhysicalHashShuffle { columns: [ #23 ] }
                        │       │   └── PhysicalHashJoin { join_type: Inner, left_keys: [ #12 ], right_keys: [ #0 ] }
                        │       │       ├── PhysicalHashShuffle { columns: [ #12 ] }
                        │       │       │   └── PhysicalProjection { exprs: [ #0, #1, #2, #3, #4, #5, #6, #7, #8, #14, #15, #16, #17, #18, #19, #20, #9, #10, #11, #12, #13 ] }
                        │       │       │       └── PhysicalHashJoin { join_type: Inner, left_keys: [ #10 ], right_keys: [ #0 ] }
                        │       │       │           ├── PhysicalHashShuffle { columns: [ #10 ] }
                        │       │       │           │   └── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ] }
                        │       │       │           │       ├── PhysicalFilter
                        │       │       │           │       │   ├── cond:And
                        │       │       │           │       │   │   ├── Eq
                        │       │       │           │       │   │   │   ├── Cast { cast_to: Int64, child: #5 }
                        │       │       │           │       │   │   │   └── 4(i64)
                        │       │       │           │       │   │   └── Like { expr: #4, pattern: "%TIN", negated: false, case_insensitive: false }
                        │       │       │           │       │   └── PhysicalHashShuffle { columns: [ #0 ] }
                        │       │       │           │       │       └── PhysicalScan { table: part }
                        │       │       │           │       └── PhysicalHashShuffle { columns: [ #0 ] }
                        │       │       │           │           └── PhysicalScan { table: partsupp }
                        │       │       │           └── PhysicalHashShuffle { columns: [ #0 ] }
                        │       │       │               └── PhysicalScan { table: supplier }
                        │       │       └── PhysicalHashShuffle { columns: [ #0 ] }
                        │       │           └── PhysicalScan { table: nation }
                        │       └── PhysicalFilter
                        │           ├── cond:Eq
                        │           │   ├── #1
                        │           │   └── "AFRICA"
                        │           └── PhysicalHashShuffle { columns: [ #0 ] }
                        │               └── PhysicalScan { table: region }
                        └── PhysicalHashShuffle { columns: [ #0 ] }
                            └── PhysicalHashJoin { join_type: Inner, left_keys: [ #14 ], right_keys: [ #0 ] }
                                ├── PhysicalHashShuffle { columns: [ #14 ] }
                                │   └── PhysicalHashJoin { join_type: Inner, left_keys: [ #8 ], right_keys: [ #0 ] }
                                │       ├── PhysicalHashShuffle { columns: [ #8 ] }
                                │       │   └── PhysicalHashJoin { join_type: Inner, left_keys: [ #1 ], right_keys: [ #0 ] }
                                │       │       ├── PhysicalHashShuffle { columns: [ #1 ] }
                                │       │       │   └── PhysicalScan { table: partsupp }
                                │       │       └── PhysicalHashShuffle { columns: [ #0 ] }
                                │       │           └── PhysicalScan { table: supplier }
                                │       └── PhysicalHashShuffle { columns: [ #0 ] }
                                │           └── PhysicalScan { table: nation }
                                └── PhysicalFilter
                                    ├── cond:Eq
                                    │   ├── #1
                                    │   └── "AFRICA"
                                    └── PhysicalHashShuffle { columns: [ #0 ] }
                                        └── PhysicalScan { table: region }
*/

